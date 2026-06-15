use caml::{CamlCompiler, CamlManifest, CompileError};

#[test]
fn rejects_duplicate_pipeline_ids() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "dup"
    input: "rtsp://127.0.0.1:8554/one"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
  - id: "dup"
    input: "rtsp://127.0.0.1:8554/two"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .expect("manifest should parse");

    let error = CamlCompiler::compile(&manifest).expect_err("compiler should reject duplicates");
    assert_eq!(error, CompileError::DuplicatePipelineId("dup".to_string()));
}

#[test]
fn rejects_pi5_hardware_encoder() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "pi5_transcode"
    input: "/dev/video0"
    type: "device"
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

    let error = CamlCompiler::compile(&manifest).expect_err("compiler should reject encoder");
    assert!(matches!(error, CompileError::HardwareMismatch(_)));
    assert!(error.to_string().contains("Raspberry Pi 5"));
}

#[test]
fn compiles_runtime_ready_graph() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "forward_primary"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "udp"
      packet_size_limit: 1400
      stall_timeout: 5s
"#,
    )
    .expect("manifest should parse");

    let compiled = CamlCompiler::compile(&manifest).expect("compiler should succeed");

    assert_eq!(compiled.system.cma_allocation_limit_bytes, 256_000_000);
    assert_eq!(compiled.pipelines.len(), 1);
    assert_eq!(compiled.pipelines[0].runtime.buffer_size, 1400);
    assert_eq!(
        compiled.pipelines[0].runtime.watchdog_timeout,
        std::time::Duration::from_secs(5)
    );
}
