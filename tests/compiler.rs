use std::sync::Arc;

use caml::{
    CamlCompiler, CamlManifest, CompileError, CompositeCapabilityProbe, HostCapabilities, PiModel,
    StaticCapabilityProbe,
};

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

    let error = CamlCompiler::compile_unchecked(&manifest).expect_err("compiler should reject encoder");
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
    assert_eq!(
        compiled.pipelines[0].resolved_backend,
        caml::ResolvedInputBackend::FfmpegRtsp
    );
    assert_eq!(
        compiled.pipelines[0].execution_mode,
        caml::ExecutionMode::EncodedPackets
    );
}

#[test]
fn rejects_capability_requirements_when_probe_disagrees() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "v4l2_device"
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

    let probe = StaticCapabilityProbe::new(HostCapabilities::default());
    let error = CamlCompiler::compile_with_probe(&manifest, &probe)
        .expect_err("compile should reject missing V4L2 capability");

    assert!(matches!(error, CompileError::UnsupportedCapability(_)));
    assert!(error.to_string().contains("V4L2"));
}

#[test]
fn passthrough_requires_rtp_packetization_capability() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "rtsp_passthrough"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    outputs:
      - type: "webrtc_rtp"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .expect("manifest should parse");

    let probe = StaticCapabilityProbe::new(HostCapabilities {
        ffmpeg_available: true,
        rtp_packetization_available: false,
        ..HostCapabilities::default()
    });

    let error = CamlCompiler::compile_with_probe(&manifest, &probe)
        .expect_err("passthrough should require RTP packetization capability");
    assert!(matches!(error, CompileError::UnsupportedCapability(_)));
    assert!(error.to_string().contains("RTP packetization"));
}

#[test]
fn merges_capabilities_from_multiple_probes() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "rtsp_passthrough"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .expect("manifest should parse");

    let mut probe = CompositeCapabilityProbe::new();
    probe.push(Arc::new(StaticCapabilityProbe::new(HostCapabilities {
        ffmpeg_available: true,
        ..HostCapabilities::default()
    })));
    probe.push(Arc::new(StaticCapabilityProbe::new(HostCapabilities {
        rtp_packetization_available: true,
        ..HostCapabilities::default()
    })));

    let compiled =
        CamlCompiler::compile_with_probe(&manifest, &probe).expect("merged probe should compile");
    assert_eq!(compiled.pipelines.len(), 1);
}

#[test]
fn rejects_pi_target_when_probe_detects_a_different_pi_model() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "decode_pipeline"
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

    let probe = StaticCapabilityProbe::new(HostCapabilities {
        ffmpeg_available: true,
        pi_model: Some(PiModel::Pi4),
        has_pi5_stateless_decoder: true,
        ..HostCapabilities::default()
    });

    let error = CamlCompiler::compile_with_probe(&manifest, &probe)
        .expect_err("mismatched Pi host should fail");
    assert!(matches!(error, CompileError::UnsupportedCapability(_)));
    assert!(error.to_string().contains("detected a Raspberry Pi 4 host"));
}

#[test]
fn compiles_hardware_decode_with_processing_profile() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "decode_pipeline"
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

    let compiled = CamlCompiler::compile_unchecked(&manifest).expect("compiler should succeed");
    assert_eq!(
        compiled.pipelines[0].codec_path,
        caml::CodecPath::HardwareDecode
    );
}

#[test]
fn assigns_adapter_specific_recovery_classes() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "rtsp_primary"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
  - id: "libcamera_capture"
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
      bitrate: "512k"
  - id: "pi5_decode"
    input: "rtsp://127.0.0.1:8554/h265"
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

    let compiled = CamlCompiler::compile_unchecked(&manifest).expect("compiler should succeed");
    assert_eq!(
        compiled.pipelines[0].recovery.class,
        caml::RecoveryClass::Network
    );
    assert_eq!(
        compiled.pipelines[1].recovery.class,
        caml::RecoveryClass::Device
    );
    assert_eq!(
        compiled.pipelines[2].recovery.class,
        caml::RecoveryClass::Hardware
    );
}
