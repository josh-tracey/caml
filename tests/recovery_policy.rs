use std::time::Duration;

use caml::{CamlCompiler, CamlManifest, RecoveryClass};

#[test]
fn test_recovery_policy_class_defaults() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "net_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
  - id: "dev_pipeline"
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
      rotation: 90
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile(&manifest).unwrap();
    assert_eq!(compiled.pipelines.len(), 2);

    // Network recovery class asserts
    let net_p = &compiled.pipelines[0];
    assert_eq!(net_p.recovery.class, RecoveryClass::Network);
    assert_eq!(net_p.recovery.initial_backoff, Duration::from_millis(250));
    assert_eq!(net_p.recovery.max_backoff, Duration::from_secs(5));
    assert_eq!(net_p.recovery.backoff_multiplier, 2.0);
    assert_eq!(net_p.recovery.reset_after, Duration::from_secs(30));
    assert_eq!(net_p.recovery.max_restarts, 5);

    // Device recovery class asserts
    let dev_p = &compiled.pipelines[1];
    assert_eq!(dev_p.recovery.class, RecoveryClass::Device);
    assert_eq!(dev_p.recovery.initial_backoff, Duration::from_secs(1));
    assert_eq!(dev_p.recovery.max_backoff, Duration::from_secs(30));
    assert_eq!(dev_p.recovery.backoff_multiplier, 2.0);
    assert_eq!(dev_p.recovery.reset_after, Duration::from_secs(60));
    assert_eq!(dev_p.recovery.max_restarts, 3);
}

#[test]
fn test_recovery_policy_hardware_class_defaults() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "hw_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "hardware_decode"
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
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile(&manifest).unwrap();
    assert_eq!(compiled.pipelines.len(), 1);

    // Hardware recovery class asserts
    let hw_p = &compiled.pipelines[0];
    assert_eq!(hw_p.recovery.class, RecoveryClass::Hardware);
    assert_eq!(hw_p.recovery.initial_backoff, Duration::from_secs(2));
    assert_eq!(hw_p.recovery.max_backoff, Duration::from_secs(60));
    assert_eq!(hw_p.recovery.backoff_multiplier, 2.0);
    assert_eq!(hw_p.recovery.reset_after, Duration::from_secs(120));
    assert_eq!(hw_p.recovery.max_restarts, 3);
}
