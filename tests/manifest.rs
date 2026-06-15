use std::io::Cursor;

use caml::{
    CamlManifest, HardwareTarget, InputBackend, InputType, ManifestError, StreamStrategy, Transport,
};

fn valid_manifest() -> &'static str {
    r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "forward_primary"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
  - id: "belly_optical"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
      rotation: 90
"#
}

#[test]
fn parses_valid_manifest_into_typed_values() {
    let manifest = CamlManifest::from_yaml_str(valid_manifest()).expect("manifest should parse");

    assert_eq!(
        manifest.system.hardware_target,
        HardwareTarget::RaspberryPi5
    );
    assert_eq!(manifest.system.cma_allocation_limit.unwrap().as_bytes(), 512_000_000);
    assert_eq!(manifest.pipelines[0].input_type, InputType::Rtsp);
    assert_eq!(manifest.pipelines[0].strategy, StreamStrategy::Passthrough);
    assert_eq!(
        manifest.pipelines[0]
            .network
            .as_ref()
            .expect("network")
            .transport,
        Transport::Tcp
    );
    assert_eq!(
        manifest.pipelines[0]
            .network
            .as_ref()
            .expect("network")
            .stall_timeout,
        std::time::Duration::from_secs(10)
    );
    assert_eq!(
        manifest.pipelines[1]
            .processing
            .as_ref()
            .expect("processing")
            .bitrate
            .as_bits_per_second(),
        512_000
    );
}

#[test]
fn reads_manifest_from_reader() {
    let manifest = CamlManifest::from_reader(Cursor::new(valid_manifest()))
        .expect("manifest should load from reader");

    assert_eq!(manifest.pipelines.len(), 2);
}

#[test]
fn parses_optional_backend_selection() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "device"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
    backend: "v4l2"
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
"#,
    )
    .expect("manifest should parse");

    assert_eq!(manifest.pipelines[0].backend, Some(InputBackend::V4l2));
}

#[test]
fn rejects_invalid_duration_format() {
    let manifest = valid_manifest().replace("10s", "\"ten seconds\"");
    let error = CamlManifest::from_yaml_str(&manifest).expect_err("duration should fail");

    assert!(matches!(error, ManifestError::Yaml(_)));
    assert!(error.to_string().contains("Invalid duration format"));
}

#[test]
fn rejects_invalid_bitrate_format() {
    let manifest = valid_manifest().replace("\"512k\"", "\"512elephants\"");
    let error = CamlManifest::from_yaml_str(&manifest).expect_err("bitrate should fail");

    assert!(matches!(error, ManifestError::Yaml(_)));
    assert!(error.to_string().contains("Invalid bitrate format"));
}

#[test]
fn rejects_invalid_byte_size_format() {
    let manifest = valid_manifest().replace("\"512MB\"", "\"512XB\"");
    let error = CamlManifest::from_yaml_str(&manifest).expect_err("byte size should fail");

    assert!(matches!(error, ManifestError::Yaml(_)));
    assert!(error.to_string().contains("Invalid byte size format"));
}

#[test]
fn rejects_passthrough_pipeline_with_processing() {
    let manifest = r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "bad_passthrough"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
"#;

    let error = CamlManifest::from_yaml_str(manifest).expect_err("manifest should be invalid");
    assert!(matches!(error, ManifestError::Validation(_)));
    assert!(error.to_string().contains("processing"));
}

#[test]
fn rejects_transcode_pipeline_without_processing() {
    let manifest = r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "bad_transcode"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
"#;

    let error = CamlManifest::from_yaml_str(manifest).expect_err("manifest should be invalid");
    assert!(matches!(error, ManifestError::Validation(_)));
    assert!(error.to_string().contains("processing"));
}

#[test]
fn rejects_hardware_decode_pipeline_without_processing() {
    let manifest = r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "bad_hw_decode"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "hardware_decode"
"#;

    let error = CamlManifest::from_yaml_str(manifest).expect_err("manifest should be invalid");
    assert!(matches!(error, ManifestError::Validation(_)));
    assert!(error.to_string().contains("processing"));
}
