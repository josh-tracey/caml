use caml::compiler::{HostCapabilities, StaticCapabilityProbe};
use caml::{CamlError, CamlPipeline, HardwareTarget};
use std::io::Write;
use std::sync::Arc;

#[tokio::test]
async fn test_facade_builder_generic_linux_success() {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#;

    let builder = CamlPipeline::from_manifest_str(manifest_str).expect("should parse");
    let probe = Arc::new(StaticCapabilityProbe::new(HostCapabilities {
        ffmpeg_available: true,
        rtp_packetization_available: true,
        ..HostCapabilities::default()
    }));
    let builder = builder.with_capability_probe(probe).with_native_adapters();

    let res = builder.start().await;
    match res {
        Ok(_) => {}
        Err(CamlError::Builder(ref e)) => {
            assert!(
                e.to_string().contains("runtime factory is required")
                    || e.to_string().contains("missing adapter")
            );
        }
        Err(CamlError::MissingAdapter { .. }) => {}
        Err(e) => {
            panic!("unexpected error: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_facade_builder_missing_capability_probe_error() {
    let manifest_str = r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "pi_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#;

    let builder = CamlPipeline::from_manifest_str(manifest_str).expect("should parse");
    let res = builder.start().await;
    match res {
        Err(CamlError::MissingCapabilityProbe { hardware_target }) => {
            assert_eq!(hardware_target, HardwareTarget::RaspberryPi4);
        }
        other => panic!("expected MissingCapabilityProbe error, got {:?}", other),
    }
}

#[test]
fn test_facade_builder_from_file() {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#;

    let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
    write!(tmp_file, "{}", manifest_str).unwrap();

    let builder = CamlPipeline::from_manifest_file(tmp_file.path());
    assert!(builder.is_ok());
}
