use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc};

use caml_core::{
    CamlCompiler, CamlManifest, CapabilityProbe, CompileError, CompiledGraph, RuntimeEngine,
    RuntimeFactory, RuntimeHandle,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeBuilderError {
    #[error("failed to open runtime input: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Manifest(#[from] caml_core::ManifestError),
    #[error(transparent)]
    Compile(#[from] CompileError),
    #[error(transparent)]
    Runtime(#[from] caml_core::RuntimeError),
    #[error("a manifest or compiled graph is required before compile/start")]
    MissingManifest,
    #[error("runtime factory is required before starting the graph")]
    MissingRuntimeFactory,
}

#[derive(Default)]
pub struct RuntimeBuilder {
    manifest: Option<CamlManifest>,
    compiled: Option<CompiledGraph>,
    capability_probe: Option<Arc<dyn CapabilityProbe>>,
    runtime_factory: Option<RuntimeFactory>,
    #[cfg(feature = "webrtc")]
    webrtc_tracks: std::collections::HashMap<String, Arc<caml_webrtc::TrackLocalStaticRTP>>,
    #[cfg(feature = "pi")]
    libcamera_provider_factory: Option<Arc<dyn caml_linux_media::LibcameraProviderFactory>>,
    metrics: Option<Arc<dyn caml_core::metrics::MetricsExporter>>,
    overlay_variables: HashMap<String, String>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_manifest(manifest: CamlManifest) -> Self {
        Self::new().with_manifest(manifest)
    }

    pub fn from_yaml_str(input: &str) -> Result<Self, RuntimeBuilderError> {
        Ok(Self::from_manifest(CamlManifest::from_yaml_str(input)?))
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self, RuntimeBuilderError> {
        Ok(Self::from_manifest(CamlManifest::from_reader(reader)?))
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, RuntimeBuilderError> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }

    pub fn with_manifest(mut self, manifest: CamlManifest) -> Self {
        self.manifest = Some(manifest);
        self
    }

    pub fn with_compiled_graph(mut self, compiled: CompiledGraph) -> Self {
        self.compiled = Some(compiled);
        self
    }

    pub fn with_capability_probe(mut self, capability_probe: Arc<dyn CapabilityProbe>) -> Self {
        self.capability_probe = Some(capability_probe);
        self
    }

    #[cfg(any(feature = "ffmpeg", feature = "webrtc", feature = "pi"))]
    pub fn with_feature_capability_probe(mut self) -> Self {
        let mut probe = caml_core::CompositeCapabilityProbe::new();

        #[cfg(feature = "ffmpeg")]
        probe.push(Arc::new(caml_ffmpeg::ffmpeg_capabilities()));

        #[cfg(feature = "webrtc")]
        probe.push(Arc::new(caml_webrtc::webrtc_capabilities()));

        #[cfg(feature = "pi")]
        probe.push(Arc::new(caml_linux_media::linux_capability_probe()));

        self.capability_probe = Some(Arc::new(probe));
        self
    }

    #[cfg(feature = "webrtc")]
    pub fn with_webrtc_track(
        mut self,
        pipeline_id: impl Into<String>,
        track: Arc<caml_webrtc::TrackLocalStaticRTP>,
    ) -> Self {
        self.webrtc_tracks.insert(pipeline_id.into(), track);
        self
    }

    #[cfg(feature = "pi")]
    pub fn with_libcamera_provider_factory(
        mut self,
        provider_factory: Arc<dyn caml_linux_media::LibcameraProviderFactory>,
    ) -> Self {
        self.libcamera_provider_factory = Some(provider_factory);
        self
    }

    #[cfg(any(feature = "ffmpeg", feature = "pi", feature = "webrtc"))]
    pub fn with_feature_media_adapters(mut self) -> Self {
        let mut adapters = crate::adapters::BuiltinAdapters::default();

        #[cfg(feature = "ffmpeg")]
        {
            adapters.ffmpeg_source = Some(Arc::new(caml_ffmpeg::FfmpegSourceFactory::new()));
        }

        #[cfg(feature = "pi")]
        {
            if let Some(factory) = &self.libcamera_provider_factory {
                adapters.libcamera_source = Some(Arc::new(
                    caml_linux_media::LibcameraSourceFactory::new(factory.clone()),
                ));
            }
        }

        #[cfg(feature = "webrtc")]
        {
            for (pipeline_id, track) in &self.webrtc_tracks {
                adapters.webrtc_sinks.insert(
                    pipeline_id.clone(),
                    Arc::new(caml_webrtc::WebRtcSinkFactory::new(track.clone())),
                );
            }
        }

        self.runtime_factory = Some(caml_core::RuntimeFactory::new(Arc::new(adapters)));
        self
    }

    pub fn with_runtime_factory<F>(mut self, runtime_factory: F) -> Self
    where
        F: Into<RuntimeFactory>,
    {
        self.runtime_factory = Some(runtime_factory.into());
        self
    }

    pub fn with_metrics_exporter(
        mut self,
        metrics: Arc<dyn caml_core::metrics::MetricsExporter>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_overlay_variable(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.overlay_variables.insert(key.into(), value.into());
        self
    }

    pub fn with_overlay_variables<I, K, V>(mut self, variables: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in variables {
            self.overlay_variables.insert(key.into(), value.into());
        }
        self
    }

    pub fn compile(mut self) -> Result<Self, RuntimeBuilderError> {
        if self.compiled.is_none() {
            let manifest = self
                .manifest
                .as_ref()
                .ok_or(RuntimeBuilderError::MissingManifest)?;

            let compiled = if let Some(capability_probe) = self.capability_probe.as_ref() {
                CamlCompiler::compile_with_probe_and_overlay_variables(
                    manifest,
                    capability_probe.as_ref(),
                    &self.overlay_variables,
                )?
            } else {
                CamlCompiler::compile_with_overlay_variables(manifest, &self.overlay_variables)?
            };
            self.compiled = Some(compiled);
        }

        Ok(self)
    }

    pub async fn start(self) -> Result<RuntimeHandle, RuntimeBuilderError> {
        let compiled = if let Some(compiled) = self.compiled {
            compiled
        } else {
            let manifest = self
                .manifest
                .as_ref()
                .ok_or(RuntimeBuilderError::MissingManifest)?;
            if let Some(capability_probe) = self.capability_probe.as_ref() {
                CamlCompiler::compile_with_probe_and_overlay_variables(
                    manifest,
                    capability_probe.as_ref(),
                    &self.overlay_variables,
                )?
            } else {
                CamlCompiler::compile_with_overlay_variables(manifest, &self.overlay_variables)?
            }
        };

        let runtime_factory = self
            .runtime_factory
            .ok_or(RuntimeBuilderError::MissingRuntimeFactory)?;

        Ok(RuntimeEngine::start(compiled, runtime_factory, self.metrics).await?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CamlError {
    #[error(transparent)]
    Manifest(#[from] caml_core::error::ManifestError),
    #[error(transparent)]
    Compile(#[from] caml_core::error::CompileError),
    #[error(transparent)]
    Runtime(#[from] caml_core::error::RuntimeError),
    #[error(transparent)]
    Builder(#[from] RuntimeBuilderError),
    #[error("missing capability probe for hardware target {hardware_target:?}")]
    MissingCapabilityProbe {
        hardware_target: caml_core::frontend::HardwareTarget,
    },
    #[error("missing WebRTC track for pipeline {pipeline_id}")]
    MissingWebRtcTrack { pipeline_id: String },
    #[error("missing libcamera provider factory for pipeline {pipeline_id}")]
    MissingLibcameraProvider { pipeline_id: String },
    #[error("missing adapter for pipeline {pipeline_id}, backend {backend}")]
    MissingAdapter {
        pipeline_id: String,
        backend: String,
    },
    #[error("unsupported output for pipeline {pipeline_id}: {output}")]
    UnsupportedOutput { pipeline_id: String, output: String },
}

pub struct CamlPipeline;

impl CamlPipeline {
    pub fn from_manifest_file(path: impl AsRef<Path>) -> Result<CamlPipelineBuilder, CamlError> {
        let file = File::open(path).map_err(caml_core::error::ManifestError::Io)?;
        let manifest = CamlManifest::from_reader(file)?;
        Ok(CamlPipelineBuilder::new(manifest))
    }

    pub fn from_manifest_str(input: &str) -> Result<CamlPipelineBuilder, CamlError> {
        let manifest = CamlManifest::from_yaml_str(input)?;
        Ok(CamlPipelineBuilder::new(manifest))
    }
}

pub struct CamlPipelineBuilder {
    manifest: CamlManifest,
    capability_probe: Option<Arc<dyn CapabilityProbe>>,
    #[cfg(feature = "webrtc")]
    webrtc_tracks: std::collections::HashMap<String, Arc<caml_webrtc::TrackLocalStaticRTP>>,
    #[cfg(feature = "pi")]
    libcamera_provider_factory: Option<Arc<dyn caml_linux_media::LibcameraProviderFactory>>,
    use_native_adapters: bool,
    metrics: Option<Arc<dyn caml_core::metrics::MetricsExporter>>,
    overlay_variables: HashMap<String, String>,
}

impl CamlPipelineBuilder {
    pub fn new(manifest: CamlManifest) -> Self {
        Self {
            manifest,
            capability_probe: None,
            #[cfg(feature = "webrtc")]
            webrtc_tracks: std::collections::HashMap::new(),
            #[cfg(feature = "pi")]
            libcamera_provider_factory: None,
            use_native_adapters: false,
            metrics: None,
            overlay_variables: HashMap::new(),
        }
    }

    pub fn with_capability_probe(mut self, capability_probe: Arc<dyn CapabilityProbe>) -> Self {
        self.capability_probe = Some(capability_probe);
        self
    }

    pub fn with_feature_capability_probe(mut self) -> Self {
        #[allow(unused_mut)]
        let mut probe = caml_core::compiler::CompositeCapabilityProbe::new();

        #[cfg(feature = "ffmpeg")]
        probe.push(Arc::new(caml_ffmpeg::ffmpeg_capabilities()));

        #[cfg(feature = "webrtc")]
        probe.push(Arc::new(caml_webrtc::webrtc_capabilities()));

        #[cfg(feature = "pi")]
        probe.push(Arc::new(caml_linux_media::linux_capability_probe()));

        self.capability_probe = Some(Arc::new(probe));
        self
    }

    #[cfg(feature = "webrtc")]
    pub fn with_webrtc_track(
        mut self,
        pipeline_id: impl Into<String>,
        track: Arc<caml_webrtc::TrackLocalStaticRTP>,
    ) -> Self {
        self.webrtc_tracks.insert(pipeline_id.into(), track);
        self
    }

    #[cfg(feature = "pi")]
    pub fn with_libcamera_provider_factory(
        mut self,
        provider_factory: Arc<dyn caml_linux_media::LibcameraProviderFactory>,
    ) -> Self {
        self.libcamera_provider_factory = Some(provider_factory);
        self
    }

    pub fn with_native_adapters(mut self) -> Self {
        self.use_native_adapters = true;
        self
    }

    pub fn with_metrics_exporter(
        mut self,
        metrics: Arc<dyn caml_core::metrics::MetricsExporter>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_overlay_variable(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.overlay_variables.insert(key.into(), value.into());
        self
    }

    pub fn with_overlay_variables<I, K, V>(mut self, variables: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in variables {
            self.overlay_variables.insert(key.into(), value.into());
        }
        self
    }

    pub async fn start(self) -> Result<CamlRuntime, CamlError> {
        let compiled = if let Some(ref probe) = self.capability_probe {
            CamlCompiler::compile_with_probe_and_overlay_variables(
                &self.manifest,
                probe.as_ref(),
                &self.overlay_variables,
            )?
        } else {
            if self.manifest.system.hardware_target
                != caml_core::frontend::HardwareTarget::GenericLinux
            {
                return Err(CamlError::MissingCapabilityProbe {
                    hardware_target: self.manifest.system.hardware_target,
                });
            }
            CamlCompiler::compile_with_overlay_variables(&self.manifest, &self.overlay_variables)?
        };

        for pipeline in &compiled.pipelines {
            let has_webrtc_output = pipeline
                .outputs
                .iter()
                .any(|o| matches!(o, caml_core::frontend::OutputProfile::WebrtcRtp { .. }));
            if has_webrtc_output {
                #[cfg(feature = "webrtc")]
                {
                    if !self.webrtc_tracks.contains_key(&pipeline.id) {
                        return Err(CamlError::MissingWebRtcTrack {
                            pipeline_id: pipeline.id.clone(),
                        });
                    }
                }
                #[cfg(not(feature = "webrtc"))]
                {
                    return Err(CamlError::UnsupportedOutput {
                        pipeline_id: pipeline.id.clone(),
                        output: "webrtc_rtp".to_string(),
                    });
                }
            }

            if pipeline.resolved_backend == caml_core::ResolvedInputBackend::LibcameraDevice {
                #[cfg(feature = "pi")]
                {
                    if self.libcamera_provider_factory.is_none() {
                        return Err(CamlError::MissingLibcameraProvider {
                            pipeline_id: pipeline.id.clone(),
                        });
                    }
                }
                #[cfg(not(feature = "pi"))]
                {
                    return Err(CamlError::MissingAdapter {
                        pipeline_id: pipeline.id.clone(),
                        backend: "libcamera".to_string(),
                    });
                }
            }
        }

        #[allow(unused_mut)]
        let mut builder = RuntimeBuilder::new().with_compiled_graph(compiled);

        if let Some(ref metrics) = self.metrics {
            builder = builder.with_metrics_exporter(metrics.clone());
        }

        if self.use_native_adapters {
            #[cfg(feature = "webrtc")]
            {
                for (pipeline_id, track) in &self.webrtc_tracks {
                    builder = builder.with_webrtc_track(pipeline_id.clone(), track.clone());
                }
            }

            #[cfg(feature = "pi")]
            {
                if let Some(ref factory) = self.libcamera_provider_factory {
                    builder = builder.with_libcamera_provider_factory(factory.clone());
                }
            }

            #[cfg(any(feature = "ffmpeg", feature = "pi", feature = "webrtc"))]
            {
                builder = builder.with_feature_media_adapters();
            }
        }

        let handle = builder.start().await?;
        Ok(CamlRuntime { handle })
    }
}

pub struct CamlRuntime {
    handle: RuntimeHandle,
}

impl std::fmt::Debug for CamlRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CamlRuntime").finish()
    }
}

impl CamlRuntime {
    pub async fn shutdown(&self) -> Result<(), CamlError> {
        self.handle.shutdown().await.map_err(CamlError::from)
    }

    pub async fn status(&self) -> caml_core::runtime::RuntimeStatus {
        self.handle.status().await
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<caml_core::runtime::RuntimeEvent> {
        self.handle.subscribe()
    }
}
