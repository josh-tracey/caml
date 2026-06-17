use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use caml::runtime::mock::{
    MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
};
use caml::{
    runtime::RuntimeEvent, CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeEngine, TaskStatus,
};

#[tokio::test]
async fn test_chaos_multi_class_recovery() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "net_chaos"
    input: "rtsp://mock"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "udp"
      packet_size_limit: 1200
      stall_timeout: "5s"
  - id: "dev_chaos"
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
    .expect("manifest should parse");

    let mut compiled = CamlCompiler::compile(&manifest).expect("should compile");

    // Override policies to make the test run fast
    for pipeline in &mut compiled.pipelines {
        pipeline.runtime.watchdog_timeout = Duration::from_millis(15);
        pipeline.recovery.initial_backoff = Duration::from_millis(5);
        pipeline.recovery.max_backoff = Duration::from_millis(5);
        pipeline.recovery.backoff_multiplier = 1.0;
        pipeline.recovery.max_restarts = 5;
    }

    // net_chaos: stalls once, errors once, then exits gracefully
    let net_actions = vec![
        MockSourceAction::Packet(vec![1, 2, 3]),
        MockSourceAction::Stall,
        MockSourceAction::Packet(vec![4, 5, 6]),
        MockSourceAction::Fail("connection dropped".to_string()),
        MockSourceAction::Packet(vec![7, 8, 9]),
        MockSourceAction::EndOfStream,
    ];

    // dev_chaos: errors twice, then exits gracefully
    let dev_actions = vec![
        MockSourceAction::Packet(vec![10, 11, 12]),
        MockSourceAction::Fail("device disconnected".to_string()),
        MockSourceAction::Packet(vec![13, 14, 15]),
        MockSourceAction::Fail("device power cycle".to_string()),
        MockSourceAction::Packet(vec![16, 17, 18]),
        MockSourceAction::EndOfStream,
    ];

    let plans = HashMap::from([
        ("net_chaos".to_string(), MockSourcePlan::new(net_actions)),
        ("dev_chaos".to_string(), MockSourcePlan::new(dev_actions)),
    ]);

    let net_recorder = MockSinkRecorder::default();
    let dev_recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([
        ("net_chaos".to_string(), net_recorder.clone()),
        ("dev_chaos".to_string(), dev_recorder.clone()),
    ]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .expect("runtime should start");

    let mut events = handle.subscribe();
    let mut net_recoveries = 0;
    let mut dev_recoveries = 0;
    let mut finished = 0;

    while let Ok(event) = events.recv().await {
        if let RuntimeEvent::StatusChanged {
            pipeline_id,
            status,
            ..
        } = event
        {
            if status == TaskStatus::Recovering {
                if pipeline_id == "net_chaos" {
                    net_recoveries += 1;
                } else if pipeline_id == "dev_chaos" {
                    dev_recoveries += 1;
                }
            } else if status == TaskStatus::Stopped || status == TaskStatus::Failed {
                finished += 1;
                if finished >= 2 {
                    break;
                }
            }
        }
    }

    assert_eq!(net_recoveries, 2, "net_chaos should recover exactly twice");
    assert_eq!(dev_recoveries, 2, "dev_chaos should recover exactly twice");

    // Verify all payloads were processed after recoveries
    let net_payloads = net_recorder.payloads().await;
    let dev_payloads = dev_recorder.payloads().await;

    assert!(net_payloads.contains(&vec![1, 2, 3]));
    assert!(net_payloads.contains(&vec![4, 5, 6]));
    assert!(net_payloads.contains(&vec![7, 8, 9]));

    assert!(dev_payloads.contains(&vec![10, 11, 12]));
    assert!(dev_payloads.contains(&vec![13, 14, 15]));
    assert!(dev_payloads.contains(&vec![16, 17, 18]));

    handle.shutdown().await.expect("shutdown should succeed");
}
