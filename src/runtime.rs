use std::{collections::BTreeMap, future::pending, sync::Arc};

use async_trait::async_trait;
use tokio::{
    sync::{broadcast, Mutex, RwLock},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    compiler::{CompiledGraph, CompiledPipeline},
    error::RuntimeError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Spawning,
    Running,
    Stalled,
    Recovering,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEvent {
    pub pipeline_id: String,
    pub status: TaskStatus,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeStatus {
    pub pipelines: BTreeMap<String, TaskStatus>,
}

impl RuntimeStatus {
    pub fn pipeline(&self, pipeline_id: &str) -> Option<TaskStatus> {
        self.pipelines.get(pipeline_id).copied()
    }
}

#[async_trait]
pub trait MediaSource: Send {
    async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, RuntimeError>;
}

#[async_trait]
pub trait MediaSink: Send {
    async fn send(&mut self, payload: &[u8]) -> Result<(), RuntimeError>;
}

#[async_trait]
pub trait SourceFactory: Send + Sync {
    async fn build_source(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSource>, RuntimeError>;
}

#[async_trait]
pub trait SinkFactory: Send + Sync {
    async fn build_sink(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSink>, RuntimeError>;
}

#[derive(Clone)]
pub struct RuntimeAdapters {
    pub source_factory: Arc<dyn SourceFactory>,
    pub sink_factory: Arc<dyn SinkFactory>,
}

impl RuntimeAdapters {
    pub fn new(source_factory: Arc<dyn SourceFactory>, sink_factory: Arc<dyn SinkFactory>) -> Self {
        Self {
            source_factory,
            sink_factory,
        }
    }
}

pub struct RuntimeEngine;

impl RuntimeEngine {
    pub async fn start(
        graph: CompiledGraph,
        adapters: RuntimeAdapters,
    ) -> Result<RuntimeHandle, RuntimeError> {
        let mut initial_statuses = BTreeMap::new();
        for pipeline in &graph.pipelines {
            initial_statuses.insert(pipeline.id.clone(), TaskStatus::Spawning);
        }

        let (events_tx, _) = broadcast::channel(256);
        let inner = Arc::new(RuntimeHandleInner {
            cancellation: CancellationToken::new(),
            statuses: RwLock::new(initial_statuses),
            events_tx,
            join_handles: Mutex::new(Vec::with_capacity(graph.pipelines.len())),
        });

        for pipeline in graph.pipelines {
            publish_status(&inner, &pipeline.id, TaskStatus::Spawning, None).await;

            let source = adapters
                .source_factory
                .build_source(&pipeline)
                .await
                .map_err(|error| RuntimeError::adapter(format!("{}: {}", pipeline.id, error)))?;
            let sink = adapters
                .sink_factory
                .build_sink(&pipeline)
                .await
                .map_err(|error| RuntimeError::adapter(format!("{}: {}", pipeline.id, error)))?;

            let task_inner = Arc::clone(&inner);
            let cancellation = inner.cancellation.child_token();

            let join_handle = tokio::spawn(run_pipeline_task(
                task_inner,
                cancellation,
                pipeline,
                source,
                sink,
            ));

            inner.join_handles.lock().await.push(join_handle);
        }

        Ok(RuntimeHandle { inner })
    }
}

#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeHandleInner>,
}

impl RuntimeHandle {
    pub async fn status(&self) -> RuntimeStatus {
        RuntimeStatus {
            pipelines: self.inner.statuses.read().await.clone(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.inner.events_tx.subscribe()
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        self.inner.cancellation.cancel();

        let join_handles = {
            let mut guard = self.inner.join_handles.lock().await;
            std::mem::take(&mut *guard)
        };

        for handle in join_handles {
            handle
                .await
                .map_err(|error| RuntimeError::Join(error.to_string()))?;
        }

        Ok(())
    }
}

struct RuntimeHandleInner {
    cancellation: CancellationToken,
    statuses: RwLock<BTreeMap<String, TaskStatus>>,
    events_tx: broadcast::Sender<RuntimeEvent>,
    join_handles: Mutex<Vec<JoinHandle<()>>>,
}

async fn run_pipeline_task(
    inner: Arc<RuntimeHandleInner>,
    cancellation: CancellationToken,
    pipeline: CompiledPipeline,
    mut source: Box<dyn MediaSource>,
    mut sink: Box<dyn MediaSink>,
) {
    publish_status(&inner, &pipeline.id, TaskStatus::Running, None).await;

    let mut buffer = vec![0_u8; pipeline.runtime.buffer_size.max(1)];

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
                break;
            }
            result = timeout(pipeline.runtime.watchdog_timeout, source.recv(&mut buffer)) => {
                match result {
                    Ok(Ok(bytes_read)) => {
                        if bytes_read == 0 {
                            continue;
                        }

                        if let Err(error) = sink.send(&buffer[..bytes_read]).await {
                            publish_status(
                                &inner,
                                &pipeline.id,
                                TaskStatus::Failed,
                                Some(error.to_string()),
                            ).await;
                            break;
                        }
                    }
                    Ok(Err(error)) => {
                        publish_status(
                            &inner,
                            &pipeline.id,
                            TaskStatus::Failed,
                            Some(error.to_string()),
                        ).await;
                        break;
                    }
                    Err(_) => {
                        publish_status(
                            &inner,
                            &pipeline.id,
                            TaskStatus::Stalled,
                            Some(format!(
                                "no media received within {:?}",
                                pipeline.runtime.watchdog_timeout
                            )),
                        ).await;
                        break;
                    }
                }
            }
        }
    }
}

async fn publish_status(
    inner: &Arc<RuntimeHandleInner>,
    pipeline_id: &str,
    status: TaskStatus,
    message: Option<String>,
) {
    inner
        .statuses
        .write()
        .await
        .insert(pipeline_id.to_string(), status);
    let _ = inner.events_tx.send(RuntimeEvent {
        pipeline_id: pipeline_id.to_string(),
        status,
        message,
    });
}

pub mod mock {
    use std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
        time::Duration,
    };

    use tokio::sync::Mutex;

    use super::{
        async_trait, pending, CompiledPipeline, MediaSink, MediaSource, RuntimeError, SinkFactory,
        SourceFactory,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum MockSourceAction {
        Packet(Vec<u8>),
        Sleep(Duration),
        Stall,
        Fail(String),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct MockSourcePlan {
        actions: Vec<MockSourceAction>,
    }

    impl MockSourcePlan {
        pub fn new(actions: Vec<MockSourceAction>) -> Self {
            Self { actions }
        }
    }

    #[derive(Clone, Default)]
    pub struct MockSourceFactory {
        plans: HashMap<String, MockSourcePlan>,
    }

    impl MockSourceFactory {
        pub fn new(plans: HashMap<String, MockSourcePlan>) -> Self {
            Self { plans }
        }
    }

    #[async_trait]
    impl SourceFactory for MockSourceFactory {
        async fn build_source(
            &self,
            pipeline: &CompiledPipeline,
        ) -> Result<Box<dyn MediaSource>, RuntimeError> {
            let plan = self.plans.get(&pipeline.id).cloned().ok_or_else(|| {
                RuntimeError::adapter(format!("missing mock source plan for '{}'", pipeline.id))
            })?;

            Ok(Box::new(MockSource {
                actions: VecDeque::from(plan.actions),
            }))
        }
    }

    struct MockSource {
        actions: VecDeque<MockSourceAction>,
    }

    #[async_trait]
    impl MediaSource for MockSource {
        async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize, RuntimeError> {
            loop {
                let action = self.actions.pop_front().ok_or_else(|| {
                    RuntimeError::source("mock source exhausted its scripted actions")
                })?;

                match action {
                    MockSourceAction::Packet(payload) => {
                        if payload.len() > buffer.len() {
                            return Err(RuntimeError::source(format!(
                                "mock packet of {} bytes exceeds buffer size of {} bytes",
                                payload.len(),
                                buffer.len()
                            )));
                        }

                        buffer[..payload.len()].copy_from_slice(&payload);
                        return Ok(payload.len());
                    }
                    MockSourceAction::Sleep(duration) => tokio::time::sleep(duration).await,
                    MockSourceAction::Stall => {
                        pending::<()>().await;
                        unreachable!("pending future never resolves")
                    }
                    MockSourceAction::Fail(message) => return Err(RuntimeError::source(message)),
                }
            }
        }
    }

    #[derive(Clone, Default)]
    pub struct MockSinkRecorder {
        payloads: Arc<Mutex<Vec<Vec<u8>>>>,
        buffer_addresses: Arc<Mutex<Vec<usize>>>,
    }

    impl MockSinkRecorder {
        pub async fn payloads(&self) -> Vec<Vec<u8>> {
            self.payloads.lock().await.clone()
        }

        pub async fn buffer_addresses(&self) -> Vec<usize> {
            self.buffer_addresses.lock().await.clone()
        }
    }

    #[derive(Clone, Default)]
    pub struct MockSinkFactory {
        recorders: HashMap<String, MockSinkRecorder>,
    }

    impl MockSinkFactory {
        pub fn new(recorders: HashMap<String, MockSinkRecorder>) -> Self {
            Self { recorders }
        }
    }

    #[async_trait]
    impl SinkFactory for MockSinkFactory {
        async fn build_sink(
            &self,
            pipeline: &CompiledPipeline,
        ) -> Result<Box<dyn MediaSink>, RuntimeError> {
            let recorder = self.recorders.get(&pipeline.id).cloned().ok_or_else(|| {
                RuntimeError::adapter(format!("missing mock sink recorder for '{}'", pipeline.id))
            })?;

            Ok(Box::new(MockSink { recorder }))
        }
    }

    struct MockSink {
        recorder: MockSinkRecorder,
    }

    #[async_trait]
    impl MediaSink for MockSink {
        async fn send(&mut self, payload: &[u8]) -> Result<(), RuntimeError> {
            self.recorder
                .buffer_addresses
                .lock()
                .await
                .push(payload.as_ptr() as usize);
            self.recorder.payloads.lock().await.push(payload.to_vec());
            Ok(())
        }
    }
}
