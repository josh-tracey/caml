use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use caml::runtime::{EncodedPacket, MediaPayload, MediaSource, PipelineContext, SourceFactory, RuntimeEvent};
use caml::{
    runtime::mock::{
        MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
    },
    CamlCompiler, CamlManifest, RuntimeAdapters, RuntimeEngine, RuntimeError, TaskStatus,
};
use tokio::sync::Mutex;

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

#[tokio::test]
async fn runtime_starts_and_shuts_down_cleanly() {
    let compiled = CamlCompiler::compile(&runtime_manifest("200ms")).expect("compile should pass");

    let plans = HashMap::from([(
        "camera_a".to_string(),
        MockSourcePlan::new(vec![
            MockSourceAction::Packet(vec![1, 2, 3]),
            MockSourceAction::Sleep(Duration::from_millis(5)),
            MockSourceAction::Packet(vec![4, 5, 6]),
            MockSourceAction::Sleep(Duration::from_millis(50)),
            MockSourceAction::EndOfStream,
        ]),
    )]);

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder.clone())]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .expect("runtime should start");

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
async fn runtime_watchdog_recovers_after_a_transient_stall() {
    let compiled = CamlCompiler::compile(&runtime_manifest("25ms")).expect("compile should pass");

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder)]);
    let source_factory = SequencedSourceFactory::new(HashMap::from([(
        "camera_a".to_string(),
        vec![
            vec![SequencedAction::Stall],
            vec![
                SequencedAction::Packet(vec![9, 9, 9]),
                SequencedAction::Sleep(Duration::from_millis(5)),
                SequencedAction::EndOfStream,
            ],
        ],
    )]));

    let adapters = RuntimeAdapters::new(
        Arc::new(source_factory),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .expect("runtime should start");
    let mut events = handle.subscribe();

    let saw_recovering = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = events
                .recv()
                .await
                .expect("runtime event should be available");
            if let RuntimeEvent::StatusChanged { ref pipeline_id, status, .. } = event {
                if pipeline_id == "camera_a" && status == TaskStatus::Recovering {
                    break true;
                }
            }
        }
    })
    .await
    .expect("expected a recovering event");
    assert!(saw_recovering);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.status().await.pipeline("camera_a") == Some(TaskStatus::Stopped) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pipeline did not return to a terminal state after recovery");
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
            MockSourceAction::EndOfStream,
        ]),
    )]);

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder.clone())]);

    let adapters = RuntimeAdapters::new(
        Arc::new(MockSourceFactory::new(plans)),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
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

#[derive(Clone)]
struct SequencedSourceFactory {
    plans: Arc<Mutex<HashMap<String, Vec<Vec<SequencedAction>>>>>,
}

impl SequencedSourceFactory {
    fn new(plans: HashMap<String, Vec<Vec<SequencedAction>>>) -> Self {
        Self {
            plans: Arc::new(Mutex::new(plans)),
        }
    }
}

#[async_trait]
impl SourceFactory for SequencedSourceFactory {
    async fn build_source(
        &self,
        pipeline: &caml::CompiledPipeline,
    ) -> Result<Box<dyn MediaSource>, RuntimeError> {
        let mut plans = self.plans.lock().await;
        let actions = plans
            .get_mut(&pipeline.id)
            .and_then(|entries| {
                if entries.is_empty() {
                    None
                } else {
                    Some(entries.remove(0))
                }
            })
            .ok_or_else(|| {
                RuntimeError::adapter(format!("missing staged plan for '{}'", pipeline.id))
            })?;

        Ok(Box::new(SequencedSource { actions, offset: 0 }))
    }
}

#[derive(Clone)]
enum SequencedAction {
    Packet(Vec<u8>),
    RecoverableFail(String),
    Stall,
    Sleep(Duration),
    EndOfStream,
}

struct SequencedSource {
    actions: Vec<SequencedAction>,
    offset: usize,
}

#[async_trait]
impl MediaSource for SequencedSource {
    async fn next(&mut self, context: &mut PipelineContext) -> Result<MediaPayload, RuntimeError> {
        loop {
            let action =
                self.actions.get(self.offset).cloned().ok_or_else(|| {
                    RuntimeError::source("sequenced source exhausted its actions")
                })?;
            self.offset += 1;

            match action {
                SequencedAction::Packet(bytes) => {
                    let mut buffer = context.acquire_buffer();
                    buffer.extend_from_slice(&bytes);
                    return Ok(MediaPayload::EncodedPacket(EncodedPacket {
                        codec: "h264".to_string(),
                        timestamp: None,
                        duration: None,
                        is_keyframe: false,
                        data: caml::runtime::MediaStorage::Pooled(buffer),
                    }));
                }
                SequencedAction::RecoverableFail(message) => {
                    return Err(RuntimeError::recoverable(message));
                }
                SequencedAction::Stall => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    unreachable!("stall action should time out before returning")
                }
                SequencedAction::Sleep(duration) => tokio::time::sleep(duration).await,
                SequencedAction::EndOfStream => return Ok(MediaPayload::EndOfStream),
            }
        }
    }
}

#[tokio::test]
async fn runtime_recovers_after_a_transient_source_error() {
    let compiled = CamlCompiler::compile(&runtime_manifest("200ms")).expect("compile should pass");

    let recorder = MockSinkRecorder::default();
    let recorders = HashMap::from([("camera_a".to_string(), recorder.clone())]);
    let source_factory = SequencedSourceFactory::new(HashMap::from([(
        "camera_a".to_string(),
        vec![
            vec![SequencedAction::RecoverableFail(
                "temporary RTSP read failure".to_string(),
            )],
            vec![
                SequencedAction::Packet(vec![7, 7, 7]),
                SequencedAction::Sleep(Duration::from_millis(5)),
                SequencedAction::EndOfStream,
            ],
        ],
    )]));

    let adapters = RuntimeAdapters::new(
        Arc::new(source_factory),
        Arc::new(MockSinkFactory::new(recorders)),
    );

    let handle = RuntimeEngine::start(compiled, adapters, None)
        .await
        .expect("runtime should start");
    let mut events = handle.subscribe();

    let saw_recovering = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = events
                .recv()
                .await
                .expect("runtime event should be available");
            if let RuntimeEvent::StatusChanged { ref pipeline_id, status, .. } = event {
                if pipeline_id == "camera_a" && status == TaskStatus::Recovering {
                    break true;
                }
            }
        }
    })
    .await
    .expect("expected a recovering event");
    assert!(saw_recovering);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if recorder.payloads().await == vec![vec![7, 7, 7]] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovered pipeline did not emit payloads");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if handle.status().await.pipeline("camera_a") == Some(TaskStatus::Stopped) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pipeline did not stop after recovery");

    handle.shutdown().await.expect("shutdown should succeed");
}
