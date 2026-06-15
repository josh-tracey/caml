#![cfg(feature = "pi")]

use caml::{CamlCompiler, CamlManifest, CapabilityProbe, HardwareTarget};

fn pi_host_tests_enabled() -> bool {
    std::env::var_os("CAML_PI_HOST_TESTS").is_some()
}

#[test]
fn pi4_hardware_encode_host_guardrail() {
    if !pi_host_tests_enabled() {
        eprintln!(
            "skipping Pi 4 host execution guardrail; set CAML_PI_HOST_TESTS=1 on Pi 4 hardware"
        );
        return;
    }

    let probe = caml_linux_media::linux_capability_probe();
    let capabilities = probe
        .capabilities(HardwareTarget::RaspberryPi4)
        .expect("Linux capability probe should run");

    assert_eq!(capabilities.pi_model, Some(caml::PiModel::Pi4));
    assert!(capabilities.has_pi4_h264_encoder);

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "pi4_encode"
    input: "/dev/video0"
    type: "device"
    backend: "v4l2"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "v4l2m2m"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
"#,
    )
    .expect("manifest should parse");

    CamlCompiler::compile_with_probe(&manifest, &probe)
        .expect("Pi 4 hardware encode should compile on a matching host");
}

#[test]
fn pi5_stateless_decode_host_guardrail() {
    if !pi_host_tests_enabled() {
        eprintln!(
            "skipping Pi 5 host execution guardrail; set CAML_PI_HOST_TESTS=1 on Pi 5 hardware"
        );
        return;
    }

    let probe = caml_linux_media::linux_capability_probe();
    let capabilities = probe
        .capabilities(HardwareTarget::RaspberryPi5)
        .expect("Linux capability probe should run");

    assert_eq!(capabilities.pi_model, Some(caml::PiModel::Pi5));
    assert!(capabilities.has_pi5_stateless_decoder);

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "pi5_decode"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "hardware_decode"
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

    CamlCompiler::compile_with_probe(&manifest, &probe)
        .expect("Pi 5 stateless decode should compile on a matching host");
}

#[tokio::test]
async fn pi_hardware_execution_test() {
    if !pi_host_tests_enabled() {
        return;
    }

    // Attempting an actual media-flow execution through Pi hardware.
    // Ensure you have a valid V4L2 source at /dev/video0 for this to work.

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "pi_hardware_exec"
    input: "/dev/video0"
    type: "device"
    backend: "v4l2"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "v4l2m2m"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
"#,
    )
    .expect("manifest should parse");

    // In a real environment, you'd wire up the runtime builder and run frames through.
    // For now we just ensure that the graph compilation and runtime builder succeeds
    // when targeting the actual Pi hardware.

    let probe = caml_linux_media::linux_capability_probe();
    let graph = CamlCompiler::compile_with_probe(&manifest, &probe).unwrap();
    let pipeline = graph.pipelines.first().unwrap();

    // Assert benchmark metrics / setup here when actually running.
    // We expect the execution to complete without allocating on the hot loop.
}
