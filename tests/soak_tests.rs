use std::{collections::HashMap, sync::Arc, time::Duration};

use caml_core::{
    compiler::CamlCompiler,
    frontend::CamlManifest,
    runtime::{
        mock::{
            MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
        },
        RuntimeAdapters, RuntimeEngine, TaskStatus,
    },
};

#[tokio::test]
async fn network_stall_chaos_recovery() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "512MB"
pipelines:
  - id: "chaos_rtsp_stream"
    input: "rtsp://mock"
    type: "rtsp"
    backend: "ffmpeg"
    strategy: "passthrough"
    network:
      transport: "udp"
      packet_size_limit: 1200
      stall_timeout: "5s"
"#,
    )
    .expect("manifest should parse");

    let mut compiled = CamlCompiler::compile(&manifest).expect("should compile");
    if let Some(pipeline) = compiled.pipelines.first_mut() {
        pipeline.runtime.watchdog_timeout = Duration::from_millis(10);
        pipeline.recovery.max_restarts = 5;
        pipeline.recovery.restart_backoff = Duration::from_millis(5);
    }

    // We simulate a flaky 24-hour stream by rapidly stalling the mock network stream and triggering watchdogs
    let actions = vec![
        MockSourceAction::Packet(vec![1, 2, 3]),
        MockSourceAction::Stall, // First stall
        MockSourceAction::Packet(vec![4, 5, 6]),
        MockSourceAction::Fail("network disconnected".to_string()), // Second fail
        MockSourceAction::Packet(vec![7, 8, 9]),
        MockSourceAction::EndOfStream, // Graceful exit
    ];

    let mut plans = HashMap::new();
    plans.insert(
        "chaos_rtsp_stream".to_string(),
        MockSourcePlan::new(actions),
    );

    let mut recorders = HashMap::new();
    let recorder = MockSinkRecorder::default();
    recorders.insert("chaos_rtsp_stream".to_string(), recorder.clone());

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .expect("runtime should start");

    let mut events = handle.subscribe();
    let mut recoveries = 0;

    while let Ok(event) = events.recv().await {
        if event.status == TaskStatus::Recovering {
            recoveries += 1;
        } else if event.status == TaskStatus::Stopped || event.status == TaskStatus::Failed {
            break;
        }
    }

    // It should have recovered exactly twice before reading EOS
    assert_eq!(
        recoveries, 2,
        "should have recovered exactly twice from chaos"
    );
}
