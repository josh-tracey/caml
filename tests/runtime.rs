use std::{collections::HashMap, sync::Arc, time::Duration};

use caml::{
    runtime::mock::{
        MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
    },
    CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeEngine, TaskStatus,
};

fn runtime_manifest(stall_timeout: &str) -> CamlManifest {
    CamlManifest::from_yaml_str(&format!(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "camera_a"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: {stall_timeout}
"#
    ))
    .expect("manifest should parse")
}

async fn wait_for_status(handle: &caml::RuntimeHandle, pipeline_id: &str, expected: TaskStatus) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.status().await.pipeline(pipeline_id) == Some(expected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("status transition timed out");
}

#[tokio::test]
async fn runtime_starts_and_shuts_down_cleanly() {
    let compiled = CamlCompiler::compile(&runtime_manifest("200ms")).expect("compile should pass");

    let plans = HashMap::from([(
        "camera_a".to_string(),
        MockSourcePlan::new(vec![
            MockSourceAction::Packet(vec![1, 2, 3]),
            MockSourceAction::Sleep(Duration::from_millis(5)),
            MockSourceAction::Packet(vec![4, 5, 6]),
            MockSourceAction::Sleep(Duration::from_secs(1)),
        ]),
    )]);

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder.clone())]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters)
        .await
        .expect("runtime should start");

    wait_for_status(&handle, "camera_a", TaskStatus::Running).await;

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if recorder.payloads().await.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sink did not receive payloads");

    handle.shutdown().await.expect("shutdown should succeed");
    assert_eq!(
        handle.status().await.pipeline("camera_a"),
        Some(TaskStatus::Stopped)
    );
}

#[tokio::test]
async fn runtime_watchdog_marks_pipeline_stalled() {
    let compiled = CamlCompiler::compile(&runtime_manifest("25ms")).expect("compile should pass");

    let plans = HashMap::from([(
        "camera_a".to_string(),
        MockSourcePlan::new(vec![MockSourceAction::Stall]),
    )]);

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder)]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters)
        .await
        .expect("runtime should start");

    wait_for_status(&handle, "camera_a", TaskStatus::Stalled).await;
    handle
        .shutdown()
        .await
        .expect("shutdown should still succeed");
}

#[tokio::test]
async fn runtime_reuses_the_same_working_buffer() {
    let compiled = CamlCompiler::compile(&runtime_manifest("200ms")).expect("compile should pass");

    let plans = HashMap::from([(
        "camera_a".to_string(),
        MockSourcePlan::new(vec![
            MockSourceAction::Packet(vec![0xAA]),
            MockSourceAction::Sleep(Duration::from_millis(5)),
            MockSourceAction::Packet(vec![0xBB]),
            MockSourceAction::Sleep(Duration::from_millis(5)),
            MockSourceAction::Packet(vec![0xCC]),
            MockSourceAction::Sleep(Duration::from_secs(1)),
        ]),
    )]);

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder.clone())]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters)
        .await
        .expect("runtime should start");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if recorder.buffer_addresses().await.len() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("buffer usage did not reach expected count");

    handle.shutdown().await.expect("shutdown should succeed");

    let buffer_addresses = recorder.buffer_addresses().await;
    assert!(buffer_addresses.len() >= 3);
    assert!(buffer_addresses
        .windows(2)
        .all(|window| window[0] == window[1]));
}
