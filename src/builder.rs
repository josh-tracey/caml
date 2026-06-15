use std::{fs::File, io::Read, path::Path, sync::Arc};

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

    pub fn compile(mut self) -> Result<Self, RuntimeBuilderError> {
        if self.compiled.is_none() {
            let manifest = self
                .manifest
                .as_ref()
                .ok_or(RuntimeBuilderError::MissingManifest)?;

            let compiled = if let Some(capability_probe) = self.capability_probe.as_ref() {
                CamlCompiler::compile_with_probe(manifest, capability_probe.as_ref())?
            } else {
                CamlCompiler::compile(manifest)?
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
                CamlCompiler::compile_with_probe(manifest, capability_probe.as_ref())?
            } else {
                CamlCompiler::compile(manifest)?
            }
        };

        let runtime_factory = self
            .runtime_factory
            .ok_or(RuntimeBuilderError::MissingRuntimeFactory)?;

        Ok(RuntimeEngine::start(compiled, runtime_factory, None).await?)
    }
}
