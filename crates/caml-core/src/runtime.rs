use std::{
    collections::BTreeMap,
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::{broadcast, RwLock},
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

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Mutex<Vec<Vec<u8>>>>,
    buffer_size: usize,
}

impl BufferPool {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            buffer_size: buffer_size.max(1),
        }
    }

    pub fn acquire(&self) -> MediaBuffer {
        let mut guard = self.inner.lock().expect("buffer pool lock poisoned");
        let mut bytes = guard
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.buffer_size));
        bytes.clear();

        MediaBuffer {
            bytes,
            pool: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
pub struct MediaBuffer {
    bytes: Vec<u8>,
    pool: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MediaBuffer {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    pub fn extend_from_slice(&mut self, payload: &[u8]) {
        self.bytes.extend_from_slice(payload);
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Drop for MediaBuffer {
    fn drop(&mut self) {
        let mut bytes = std::mem::take(&mut self.bytes);
        bytes.clear();
        let mut guard = self.pool.lock().expect("buffer pool lock poisoned");
        guard.push(bytes);
    }
}

#[derive(Debug)]
pub struct EncodedPacket {
    pub codec: String,
    pub timestamp: Option<Duration>,
    pub duration: Option<Duration>,
    pub is_keyframe: bool,
    pub data: MediaBuffer,
}

#[derive(Debug)]
pub struct DecodedFrame {
    pub pixel_format: String,
    pub width: u32,
    pub height: u32,
    pub timestamp: Option<Duration>,
    pub data: MediaBuffer,
}

#[derive(Debug)]
pub enum MediaPayload {
    EncodedPacket(EncodedPacket),
    DecodedFrame(DecodedFrame),
    EndOfStream,
}

impl MediaPayload {
    pub fn data(&self) -> Option<&[u8]> {
        match self {
            Self::EncodedPacket(packet) => Some(packet.data.as_slice()),
            Self::DecodedFrame(frame) => Some(frame.data.as_slice()),
            Self::EndOfStream => None,
        }
    }

    pub fn buffer_ptr(&self) -> Option<usize> {
        self.data().map(|bytes| bytes.as_ptr() as usize)
    }
}

#[derive(Clone)]
pub struct PipelineContext {
    pub pipeline: CompiledPipeline,
    pub buffer_pool: BufferPool,
}

impl PipelineContext {
    pub fn acquire_buffer(&self) -> MediaBuffer {
        self.buffer_pool.acquire()
    }
}

#[async_trait]
pub trait MediaSource: Send {
    async fn next(&mut self, context: &mut PipelineContext) -> Result<MediaPayload, RuntimeError>;
}

#[async_trait]
pub trait MediaTransform: Send {
    async fn transform(
        &mut self,
        payload: MediaPayload,
        context: &mut PipelineContext,
    ) -> Result<MediaPayload, RuntimeError>;
}

#[async_trait]
pub trait MediaSink: Send {
    async fn consume(
        &mut self,
        payload: MediaPayload,
        context: &mut PipelineContext,
    ) -> Result<(), RuntimeError>;
}

pub struct PipelineStages {
    pub source: Box<dyn MediaSource>,
    pub transforms: Vec<Box<dyn MediaTransform>>,
    pub sink: Box<dyn MediaSink>,
}

#[async_trait]
pub trait PipelineFactory: Send + Sync {
    async fn build_pipeline(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<PipelineStages, RuntimeError>;
}

#[async_trait]
pub trait SourceFactory: Send + Sync {
    async fn build_source(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSource>, RuntimeError>;
}

#[async_trait]
pub trait TransformFactory: Send + Sync {
    async fn build_transforms(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Vec<Box<dyn MediaTransform>>, RuntimeError>;
}

#[async_trait]
pub trait SinkFactory: Send + Sync {
    async fn build_sink(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSink>, RuntimeError>;
}

#[derive(Clone, Default)]
pub struct NoopTransformFactory;

#[async_trait]
impl TransformFactory for NoopTransformFactory {
    async fn build_transforms(
        &self,
        _pipeline: &CompiledPipeline,
    ) -> Result<Vec<Box<dyn MediaTransform>>, RuntimeError> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub struct RuntimeAdapters {
    pub source_factory: Arc<dyn SourceFactory>,
    pub transform_factory: Arc<dyn TransformFactory>,
    pub sink_factory: Arc<dyn SinkFactory>,
    pub metrics_exporter: Arc<dyn crate::metrics::MetricsExporter>,
}

impl RuntimeAdapters {
    pub fn new(source_factory: Arc<dyn SourceFactory>, sink_factory: Arc<dyn SinkFactory>) -> Self {
        Self {
            source_factory,
            transform_factory: Arc::new(NoopTransformFactory),
            sink_factory,
            metrics_exporter: Arc::new(crate::metrics::NoopMetricsExporter),
        }
    }

    pub fn with_transform_factory(mut self, transform_factory: Arc<dyn TransformFactory>) -> Self {
        self.transform_factory = transform_factory;
        self
    }

    pub fn with_metrics_exporter(mut self, exporter: Arc<dyn crate::metrics::MetricsExporter>) -> Self {
        self.metrics_exporter = exporter;
        self
    }
}

#[async_trait]
impl PipelineFactory for RuntimeAdapters {
    async fn build_pipeline(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<PipelineStages, RuntimeError> {
        Ok(PipelineStages {
            source: self.source_factory.build_source(pipeline).await?,
            transforms: self.transform_factory.build_transforms(pipeline).await?,
            sink: self.sink_factory.build_sink(pipeline).await?,
        })
    }
}

#[derive(Clone)]
pub struct RuntimeFactory {
    inner: Arc<dyn PipelineFactory>,
}

impl RuntimeFactory {
    pub fn new(inner: Arc<dyn PipelineFactory>) -> Self {
        Self { inner }
    }
}

impl From<RuntimeAdapters> for RuntimeFactory {
    fn from(value: RuntimeAdapters) -> Self {
        Self::new(Arc::new(value))
    }
}

impl From<Arc<dyn PipelineFactory>> for RuntimeFactory {
    fn from(value: Arc<dyn PipelineFactory>) -> Self {
        Self::new(value)
    }
}

pub struct RuntimeEngine;

impl RuntimeEngine {
    pub async fn start<F>(
        graph: CompiledGraph,
        factory: F,
        metrics: Option<Arc<dyn crate::metrics::MetricsExporter>>
    ) -> Result<RuntimeHandle, RuntimeError>
    where
        F: Into<RuntimeFactory>,
    {
        let runtime_factory = factory.into();
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
            metrics,
        });

        for pipeline in graph.pipelines {
            publish_status(&inner, &pipeline.id, TaskStatus::Spawning, None).await;
            let task_inner = Arc::clone(&inner);
            let cancellation = inner.cancellation.child_token();
            let pipeline_factory = runtime_factory.clone();

            let join_handle = tokio::spawn(supervise_pipeline_task(
                task_inner,
                cancellation,
                pipeline_factory,
                pipeline,
            ));

            inner
                .join_handles
                .lock()
                .expect("join handle lock poisoned")
                .push(join_handle);
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
            let mut guard = self
                .inner
                .join_handles
                .lock()
                .expect("join handle lock poisoned");
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
    metrics: Option<Arc<dyn crate::metrics::MetricsExporter>>,
}

#[derive(Debug)]
enum PipelineLoopExit {
    Cancelled,
    EndOfStream,
    Recoverable(String),
    Fatal(String),
}

async fn supervise_pipeline_task(
    inner: Arc<RuntimeHandleInner>,
    cancellation: CancellationToken,
    pipeline_factory: RuntimeFactory,
    pipeline: CompiledPipeline,
) {
    let mut attempts = 0;

    loop {
        if cancellation.is_cancelled() {
            publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
            break;
        }

        let stages = match pipeline_factory.inner.build_pipeline(&pipeline).await {
            Ok(stages) => stages,
            Err(error) => {
                publish_status(
                    &inner,
                    &pipeline.id,
                    TaskStatus::Failed,
                    Some(error.to_string()),
                )
                .await;
                break;
            }
        };

        publish_status(&inner, &pipeline.id, TaskStatus::Running, None).await;

        match run_pipeline_once(cancellation.clone(), &pipeline, stages, inner.metrics.clone()).await {
            PipelineLoopExit::Cancelled | PipelineLoopExit::EndOfStream => {
                publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
                break;
            }
            PipelineLoopExit::Recoverable(message) if attempts < pipeline.recovery.max_restarts => {
                attempts += 1;
                publish_status(
                    &inner,
                    &pipeline.id,
                    TaskStatus::Stalled,
                    Some(message.clone()),
                )
                .await;
                publish_status(
                    &inner,
                    &pipeline.id,
                    TaskStatus::Recovering,
                    Some(format!("restart attempt {}", attempts)),
                )
                .await;
                
                if let Some(metrics) = inner.metrics.as_ref() {
                    metrics.record_restart(&pipeline.id, &message).await;
                }

                tokio::select! {
                    _ = cancellation.cancelled() => {
                        publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
                        break;
                    }
                    _ = tokio::time::sleep(pipeline.recovery.restart_backoff) => {}
                }
            }
            PipelineLoopExit::Recoverable(message) | PipelineLoopExit::Fatal(message) => {
                publish_status(&inner, &pipeline.id, TaskStatus::Failed, Some(message)).await;
                break;
            }
        }
    }
}

async fn run_pipeline_once(
    cancellation: CancellationToken,
    pipeline: &CompiledPipeline,
    mut stages: PipelineStages,
    metrics: Option<Arc<dyn crate::metrics::MetricsExporter>>,
) -> PipelineLoopExit {
    let mut context = PipelineContext {
        pipeline: pipeline.clone(),
        buffer_pool: BufferPool::new(pipeline.runtime.buffer_size),
    };

    loop {
        let mut payload = tokio::select! {
            _ = cancellation.cancelled() => return PipelineLoopExit::Cancelled,
            result = timeout(pipeline.runtime.watchdog_timeout, stages.source.next(&mut context)) => {
                match result {
                    Ok(Ok(payload)) => payload,
                    Ok(Err(error)) => {
                        if let Some(m) = &metrics {
                            m.record_stream_error(&pipeline.id, &error.to_string()).await;
                        }
                        return pipeline_exit_from_error(error);
                    }
                    Err(_) => {
                        return PipelineLoopExit::Recoverable(format!(
                            "no media received within {:?}",
                            pipeline.runtime.watchdog_timeout,
                        ));
                    }
                }
            }
        };

        if let Some(metrics) = &metrics {
            if let Some(data) = payload.data() {
                metrics.record_throughput(&pipeline.id, data.len()).await;
            }
        }

        if matches!(payload, MediaPayload::EndOfStream) {
            return PipelineLoopExit::EndOfStream;
        }

        for transform in &mut stages.transforms {
            match transform.transform(payload, &mut context).await {
                Ok(transformed) => payload = transformed,
                Err(error) => {
                    if let Some(m) = &metrics {
                        m.record_stream_error(&pipeline.id, &error.to_string()).await;
                    }
                    return pipeline_exit_from_error(error);
                }
            }
        }

        if let Err(error) = stages.sink.consume(payload, &mut context).await {
            if let Some(m) = &metrics {
                m.record_stream_error(&pipeline.id, &error.to_string()).await;
            }
            return pipeline_exit_from_error(error);
        }
    }
}

fn pipeline_exit_from_error(error: RuntimeError) -> PipelineLoopExit {
    let message = error.to_string();
    if error.is_recoverable() {
        PipelineLoopExit::Recoverable(message)
    } else {
        PipelineLoopExit::Fatal(message)
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
        async_trait, pending, CompiledPipeline, EncodedPacket, MediaPayload, MediaSink,
        MediaSource, PipelineContext, RuntimeError, SinkFactory, SourceFactory,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum MockSourceAction {
        Packet(Vec<u8>),
        Sleep(Duration),
        Stall,
        Fail(String),
        EndOfStream,
    }

    #[derive(Clone)]
    pub struct MockSourcePlan {
        actions: std::sync::Arc<std::sync::Mutex<VecDeque<MockSourceAction>>>,
    }

    impl MockSourcePlan {
        pub fn new(actions: Vec<MockSourceAction>) -> Self {
            Self { actions: std::sync::Arc::new(std::sync::Mutex::new(VecDeque::from(actions))) }
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
                actions: plan.actions.clone(),
            }))
        }
    }

    struct MockSource {
        actions: std::sync::Arc<std::sync::Mutex<VecDeque<MockSourceAction>>>,
    }

    #[async_trait]
    impl MediaSource for MockSource {
        async fn next(
            &mut self,
            context: &mut PipelineContext,
        ) -> Result<MediaPayload, RuntimeError> {
            loop {
                let action = {
                    let mut lock = self.actions.lock().unwrap();
                    lock.pop_front()
                }.ok_or_else(|| {
                    RuntimeError::source("mock source exhausted its scripted actions")
                })?;

                match action {
                    MockSourceAction::Packet(payload) => {
                        let mut data = context.acquire_buffer();
                        data.extend_from_slice(&payload);
                        return Ok(MediaPayload::EncodedPacket(EncodedPacket {
                            codec: "h264".to_string(),
                            timestamp: None,
                            duration: None,
                            is_keyframe: false,
                            data,
                        }));
                    }
                    MockSourceAction::Sleep(duration) => tokio::time::sleep(duration).await,
                    MockSourceAction::Stall => {
                        pending::<()>().await;
                        unreachable!("pending future never resolves")
                    }
                    MockSourceAction::Fail(message) => return Err(RuntimeError::recoverable(message)),
                    MockSourceAction::EndOfStream => return Ok(MediaPayload::EndOfStream),
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
        async fn consume(
            &mut self,
            payload: MediaPayload,
            _context: &mut PipelineContext,
        ) -> Result<(), RuntimeError> {
            if let Some(buffer_ptr) = payload.buffer_ptr() {
                self.recorder.buffer_addresses.lock().await.push(buffer_ptr);
            }
            if let Some(bytes) = payload.data() {
                self.recorder.payloads.lock().await.push(bytes.to_vec());
            }
            Ok(())
        }
    }
}
