use std::{
    fs,
    path::{Path, PathBuf},
};

use caml_core::{CapabilityProbe, CompileError, HardwareTarget, HostCapabilities, PiModel};

#[derive(Debug, Clone)]
pub struct LinuxCapabilityProbe {
    root: PathBuf,
}

impl LinuxCapabilityProbe {
    pub fn new() -> Self {
        Self {
            root: PathBuf::from("/"),
        }
    }

    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Default for LinuxCapabilityProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl CapabilityProbe for LinuxCapabilityProbe {
    fn capabilities(
        &self,
        hardware_target: HardwareTarget,
    ) -> Result<HostCapabilities, CompileError> {
        let inspector = LinuxProbeInspector::new(self.root.clone());
        let model = inspector.detect_pi_model();
        let v4l2_names = inspector.video4linux_names();
        let media_names = inspector.media_models();

        Ok(HostCapabilities {
            ffmpeg_available: false,
            v4l2_available: inspector.has_v4l2(),
            libcamera_available: inspector.has_libcamera(),
            rtp_packetization_available: false,
            pi_model: model,
            has_pi4_h264_encoder: detect_pi4_h264_encoder(model, &v4l2_names, &media_names),
            has_pi5_stateless_decoder: detect_pi5_stateless_decoder(
                hardware_target,
                model,
                &v4l2_names,
                &media_names,
                inspector.has_media_nodes(),
            ),
        })
    }
}

pub fn linux_capability_probe() -> LinuxCapabilityProbe {
    LinuxCapabilityProbe::new()
}

#[derive(Debug, Clone)]
struct LinuxProbeInspector {
    root: PathBuf,
}

impl LinuxProbeInspector {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn resolve(&self, relative: &str) -> PathBuf {
        let trimmed = relative.trim_start_matches('/');
        self.root.join(trimmed)
    }

    fn read_string(&self, relative: &str) -> Option<String> {
        fs::read_to_string(self.resolve(relative))
            .ok()
            .map(|value| value.trim_matches(char::from(0)).trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn read_dir_entries(&self, relative: &str) -> Vec<PathBuf> {
        let Ok(entries) = fs::read_dir(self.resolve(relative)) else {
            return Vec::new();
        };

        entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect()
    }

    fn detect_pi_model(&self) -> Option<PiModel> {
        let locations = [
            "/proc/device-tree/model",
            "/sys/firmware/devicetree/base/model",
        ];

        for location in locations {
            let Some(model) = self.read_string(location) else {
                continue;
            };
            let normalized = model.to_ascii_lowercase();
            if normalized.contains("raspberry pi 5") {
                return Some(PiModel::Pi5);
            }
            if normalized.contains("raspberry pi 4") {
                return Some(PiModel::Pi4);
            }
        }

        None
    }

    fn video4linux_names(&self) -> Vec<String> {
        self.read_dir_entries("/sys/class/video4linux")
            .into_iter()
            .filter_map(|entry| read_child_string(&entry, "name"))
            .collect()
    }

    fn media_models(&self) -> Vec<String> {
        self.read_dir_entries("/sys/class/media")
            .into_iter()
            .filter_map(|entry| read_child_string(&entry, "model"))
            .collect()
    }

    fn has_v4l2(&self) -> bool {
        self.resolve("/dev/video0").exists()
            || !self.read_dir_entries("/sys/class/video4linux").is_empty()
    }

    fn has_media_nodes(&self) -> bool {
        self.resolve("/dev/media0").exists()
            || !self.read_dir_entries("/sys/class/media").is_empty()
    }

    fn has_libcamera(&self) -> bool {
        self.resolve("/usr/bin/libcamera-hello").exists()
            || self.resolve("/usr/bin/libcamera-still").exists()
            || self.resolve("/usr/bin/libcamera-vid").exists()
    }
}

fn read_child_string(parent: &Path, child: &str) -> Option<String> {
    fs::read_to_string(parent.join(child))
        .ok()
        .map(|value| value.trim_matches(char::from(0)).trim().to_string())
        .filter(|value| !value.is_empty())
}

fn detect_pi4_h264_encoder(
    model: Option<PiModel>,
    v4l2_names: &[String],
    media_names: &[String],
) -> bool {
    if model != Some(PiModel::Pi4) {
        return false;
    }

    any_name_matches(v4l2_names, &["bcm2835", "codec", "encode"])
        || any_name_matches(media_names, &["bcm2835", "codec", "encode"])
        || any_name_matches(v4l2_names, &["h264", "enc"])
}

fn detect_pi5_stateless_decoder(
    hardware_target: HardwareTarget,
    model: Option<PiModel>,
    v4l2_names: &[String],
    media_names: &[String],
    has_media_nodes: bool,
) -> bool {
    if hardware_target != HardwareTarget::RaspberryPi5 && model != Some(PiModel::Pi5) {
        return false;
    }

    any_name_matches(v4l2_names, &["rpivid"])
        || any_name_matches(v4l2_names, &["stateless", "decoder"])
        || any_name_matches(media_names, &["rpivid"])
        || any_name_matches(media_names, &["pisp"])
        || (model == Some(PiModel::Pi5) && has_media_nodes)
}

fn any_name_matches(names: &[String], tokens: &[&str]) -> bool {
    names.iter().any(|name| {
        let normalized = name.to_ascii_lowercase();
        tokens.iter().all(|token| normalized.contains(token))
    })
}

#[async_trait::async_trait]
pub trait LibcameraFrameProvider: Send + Sync {
    async fn next_payload(
        &mut self,
        context: &mut caml_core::PipelineContext,
    ) -> Result<caml_core::MediaPayload, caml_core::RuntimeError>;
}

#[async_trait::async_trait]
pub trait LibcameraProviderFactory: Send + Sync {
    async fn open(
        &self,
        pipeline: &caml_core::CompiledPipeline,
    ) -> Result<Box<dyn LibcameraFrameProvider>, caml_core::RuntimeError>;
}

#[derive(Clone)]
pub struct LibcameraSourceFactory {
    provider_factory: std::sync::Arc<dyn LibcameraProviderFactory>,
}

impl LibcameraSourceFactory {
    pub fn new(provider_factory: std::sync::Arc<dyn LibcameraProviderFactory>) -> Self {
        Self { provider_factory }
    }
}

#[async_trait::async_trait]
impl caml_core::SourceFactory for LibcameraSourceFactory {
    async fn build_source(
        &self,
        pipeline: &caml_core::CompiledPipeline,
    ) -> Result<Box<dyn caml_core::MediaSource>, caml_core::RuntimeError> {
        if pipeline.input.kind != caml_core::InputType::Device
            || pipeline.resolved_backend != caml_core::ResolvedInputBackend::LibcameraDevice
        {
            return Err(caml_core::RuntimeError::adapter(format!(
                "pipeline '{}' is not a libcamera device pipeline",
                pipeline.id
            )));
        }

        let provider = self.provider_factory.open(pipeline).await?;
        Ok(Box::new(LibcameraSource { provider }))
    }
}

struct LibcameraSource {
    provider: Box<dyn LibcameraFrameProvider>,
}

#[async_trait::async_trait]
impl caml_core::MediaSource for LibcameraSource {
    async fn next(
        &mut self,
        context: &mut caml_core::PipelineContext,
    ) -> Result<caml_core::MediaPayload, caml_core::RuntimeError> {
        self.provider.next_payload(context).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use caml_core::{
        CamlCompiler, CapabilityProbe, HardwareTarget, MediaPayload, PiModel, RuntimeError,
        SourceFactory,
    };

    use super::{
        linux_capability_probe, LibcameraFrameProvider, LibcameraProviderFactory,
        LibcameraSourceFactory, LinuxCapabilityProbe,
    };

    #[test]
    fn detects_pi4_hardware_encoder_from_sysfs_names() {
        let root = temp_root("pi4-encoder");
        write_file(
            &root,
            "proc/device-tree/model",
            "Raspberry Pi 4 Model B Rev 1.5",
        );
        write_file(
            &root,
            "sys/class/video4linux/video11/name",
            "bcm2835-codec-encode",
        );
        write_file(&root, "dev/video0", "");

        let probe = LinuxCapabilityProbe::with_root(&root);
        let capabilities = probe.capabilities(HardwareTarget::RaspberryPi4).unwrap();

        assert_eq!(capabilities.pi_model, Some(PiModel::Pi4));
        assert!(capabilities.v4l2_available);
        assert!(capabilities.has_pi4_h264_encoder);
        assert!(!capabilities.has_pi5_stateless_decoder);
        cleanup(&root);
    }

    #[test]
    fn detects_pi5_stateless_decoder_from_media_topology() {
        let root = temp_root("pi5-decoder");
        write_file(&root, "proc/device-tree/model", "Raspberry Pi 5 Model B");
        write_file(&root, "sys/class/media/media0/model", "rpivid-v4l2-request");
        write_file(&root, "dev/media0", "");

        let probe = LinuxCapabilityProbe::with_root(&root);
        let capabilities = probe.capabilities(HardwareTarget::RaspberryPi5).unwrap();

        assert_eq!(capabilities.pi_model, Some(PiModel::Pi5));
        assert!(capabilities.has_pi5_stateless_decoder);
        assert!(!capabilities.has_pi4_h264_encoder);
        cleanup(&root);
    }

    #[test]
    fn detects_libcamera_binaries() {
        let root = temp_root("libcamera");
        write_file(&root, "usr/bin/libcamera-hello", "");

        let probe = LinuxCapabilityProbe::with_root(&root);
        let capabilities = probe.capabilities(HardwareTarget::GenericLinux).unwrap();

        assert!(capabilities.libcamera_available);
        cleanup(&root);
    }

    #[test]
    fn default_probe_constructor_returns_linux_probe() {
        let _probe = linux_capability_probe();
    }

    #[tokio::test]
    async fn libcamera_source_factory_streams_provider_frames() {
        let manifest = caml_core::CamlManifest::from_yaml_str(
            r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "libcam0"
    input: "/base/soc/i2c0mux/i2c@1/imx219@10"
    type: "device"
    backend: "libcamera"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: 512k
"#,
        )
        .expect("manifest should parse");
        let graph = CamlCompiler::compile(&manifest).expect("manifest should compile");
        let pipeline = graph.pipelines.first().expect("pipeline should exist");
        let factory = LibcameraSourceFactory::new(Arc::new(FakeLibcameraFactory));
        let mut source = factory
            .build_source(pipeline)
            .await
            .expect("source should build");
        let mut context = caml_core::PipelineContext {
            pipeline: pipeline.clone(),
            buffer_pool: caml_core::runtime::BufferPool::new(pipeline.runtime.buffer_size),
        };

        let payload = source
            .next(&mut context)
            .await
            .expect("frame should stream");
        match payload {
            MediaPayload::EncodedPacket(packet) => {
                assert_eq!(packet.codec, "h264");
                assert_eq!(packet.data.as_slice(), &[1, 2, 3, 4]);
            }
            other => panic!("expected encoded packet, got {other:?}"),
        }

        assert!(matches!(
            source.next(&mut context).await.expect("eos should stream"),
            MediaPayload::EndOfStream
        ));
    }

    struct FakeLibcameraFactory;

    #[async_trait::async_trait]
    impl LibcameraProviderFactory for FakeLibcameraFactory {
        async fn open(
            &self,
            _pipeline: &caml_core::CompiledPipeline,
        ) -> Result<Box<dyn LibcameraFrameProvider>, RuntimeError> {
            Ok(Box::new(FakeLibcameraProvider {
                frames: VecDeque::from([vec![1, 2, 3, 4]]),
            }))
        }
    }

    struct FakeLibcameraProvider {
        frames: VecDeque<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl LibcameraFrameProvider for FakeLibcameraProvider {
        async fn next_payload(
            &mut self,
            context: &mut caml_core::PipelineContext,
        ) -> Result<MediaPayload, RuntimeError> {
            let Some(bytes) = self.frames.pop_front() else {
                return Ok(MediaPayload::EndOfStream);
            };
            let mut data = context.acquire_buffer();
            data.extend_from_slice(&bytes);
            Ok(MediaPayload::EncodedPacket(caml_core::EncodedPacket {
                codec: "h264".to_string(),
                timestamp: Some(Duration::from_millis(1)),
                duration: Some(Duration::from_millis(33)),
                is_keyframe: true,
                data,
            }))
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("caml-{label}-{suffix}"));
        fs::create_dir_all(&root).expect("temp root should be created");
        root
    }

    fn write_file(root: &Path, relative: &str, contents: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent directory should exist");
        }
        fs::write(path, contents).expect("file should be written");
    }

    fn cleanup(root: &Path) {
        let _ = fs::remove_dir_all(root);
    }
}
