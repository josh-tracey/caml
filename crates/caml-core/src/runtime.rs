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
pub enum RuntimeEvent {
    StatusChanged {
        pipeline_id: String,
        status: TaskStatus,
        message: Option<String>,
    },
    BackendStarted {
        pipeline_id: String,
        codec_path: String,
        backend_name: String,
    },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub available: usize,
    pub in_use: usize,
    pub high_watermark: usize,
}

#[derive(Debug, Clone)]
struct PoolInner {
    buffers: Vec<Vec<u8>>,
    allocated_count: usize,
    high_watermark: usize,
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<std::sync::Mutex<PoolInner>>,
    buffer_size: usize,
}

impl BufferPool {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(PoolInner {
                buffers: Vec::new(),
                allocated_count: 0,
                high_watermark: 0,
            })),
            buffer_size: buffer_size.max(1),
        }
    }

    pub fn preallocate(&self, count: usize) {
        let mut guard = self.inner.lock().expect("buffer pool lock poisoned");
        for _ in 0..count {
            guard.buffers.push(Vec::with_capacity(self.buffer_size));
        }
        guard.allocated_count += count;
        guard.high_watermark = guard.high_watermark.max(guard.allocated_count);
    }

    pub fn stats(&self) -> PoolStats {
        let guard = self.inner.lock().expect("buffer pool lock poisoned");
        PoolStats {
            available: guard.buffers.len(),
            in_use: guard.allocated_count.saturating_sub(guard.buffers.len()),
            high_watermark: guard.high_watermark,
        }
    }

    pub fn high_watermark_bytes(&self) -> usize {
        self.stats().high_watermark * self.buffer_size
    }

    pub fn acquire(&self) -> MediaBuffer {
        let mut guard = self.inner.lock().expect("buffer pool lock poisoned");
        let mut bytes = if let Some(buf) = guard.buffers.pop() {
            buf
        } else {
            guard.allocated_count += 1;
            guard.high_watermark = guard.high_watermark.max(guard.allocated_count);
            Vec::with_capacity(self.buffer_size)
        };
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
    pool: Arc<std::sync::Mutex<PoolInner>>,
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
        guard.buffers.push(bytes);
    }
}

use std::any::Any;

#[derive(Clone)]
pub struct BorrowedMediaSlice {
    ptr: *const u8,
    len: usize,
    #[allow(dead_code)]
    owner: Arc<dyn Any + Send + Sync>,
}

unsafe impl Send for BorrowedMediaSlice {}
unsafe impl Sync for BorrowedMediaSlice {}

impl BorrowedMediaSlice {
    pub fn new<T>(data: &[u8], owner: Arc<T>) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        Self {
            ptr: data.as_ptr(),
            len: data.len(),
            owner,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl std::fmt::Debug for BorrowedMediaSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BorrowedMediaSlice")
            .field("len", &self.len)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct MappedFrameHandle {
    pub slice: BorrowedMediaSlice,
}

#[derive(Debug, Clone)]
pub struct FfmpegPacketHandle {
    pub slice: BorrowedMediaSlice,
}

#[derive(Debug)]
pub enum MediaStorage {
    Pooled(MediaBuffer),
    Owned(Vec<u8>),
    BorrowedSlice(BorrowedMediaSlice),
    MappedFrame(MappedFrameHandle),
    FfmpegPacket(FfmpegPacketHandle),
}

impl MediaStorage {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(buf) => buf.as_slice(),
            Self::Owned(vec) => vec.as_slice(),
            Self::BorrowedSlice(s) => s.as_slice(),
            Self::MappedFrame(h) => h.slice.as_slice(),
            Self::FfmpegPacket(h) => h.slice.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Pooled(buf) => buf.len(),
            Self::Owned(vec) => vec.len(),
            Self::BorrowedSlice(s) => s.len,
            Self::MappedFrame(h) => h.slice.len,
            Self::FfmpegPacket(h) => h.slice.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct EncodedPacket {
    pub codec: String,
    pub timestamp: Option<Duration>,
    pub duration: Option<Duration>,
    pub is_keyframe: bool,
    pub data: MediaStorage,
}

#[derive(Debug)]
pub struct DecodedFrame {
    pub pixel_format: String,
    pub width: u32,
    pub height: u32,
    pub timestamp: Option<Duration>,
    pub data: MediaStorage,
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
    pub metrics: Option<Arc<dyn crate::metrics::MetricsExporter>>,
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

#[derive(Debug, Clone)]
pub struct RecordedPacket {
    pub codec: String,
    pub bytes: usize,
    pub is_keyframe: bool,
    pub timestamp: Option<Duration>,
}

pub struct RecordingSink {
    pub packets: Arc<tokio::sync::Mutex<Vec<RecordedPacket>>>,
}

#[async_trait]
impl MediaSink for RecordingSink {
    async fn consume(
        &mut self,
        payload: MediaPayload,
        _context: &mut PipelineContext,
    ) -> Result<(), RuntimeError> {
        if let MediaPayload::EncodedPacket(packet) = payload {
            let mut guard = self.packets.lock().await;
            guard.push(RecordedPacket {
                codec: packet.codec,
                bytes: packet.data.len(),
                is_keyframe: packet.is_keyframe,
                timestamp: packet.timestamp,
            });
        }
        Ok(())
    }
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

    pub fn with_metrics_exporter(
        mut self,
        exporter: Arc<dyn crate::metrics::MetricsExporter>,
    ) -> Self {
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
        metrics: Option<Arc<dyn crate::metrics::MetricsExporter>>,
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
    #[allow(unused_assignments)]
    let mut last_success_start: Option<tokio::time::Instant> = None;

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
        let _ = inner.events_tx.send(RuntimeEvent::BackendStarted {
            pipeline_id: pipeline.id.clone(),
            codec_path: format!("{:?}", pipeline.codec_path),
            backend_name: format!("{:?}", pipeline.resolved_backend),
        });
        last_success_start = Some(tokio::time::Instant::now());

        match run_pipeline_once(
            cancellation.clone(),
            &pipeline,
            stages,
            inner.metrics.clone(),
        )
        .await
        {
            PipelineLoopExit::Cancelled | PipelineLoopExit::EndOfStream => {
                publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
                break;
            }
            PipelineLoopExit::Recoverable(message) => {
                // If the pipeline ran stably for at least `reset_after`, reset recovery attempts
                if let Some(start_time) = last_success_start {
                    if start_time.elapsed() >= pipeline.recovery.reset_after {
                        attempts = 0;
                    }
                }

                if attempts < pipeline.recovery.max_restarts {
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
                        let formatted_message = format!("{:?}: {}", pipeline.recovery.class, message);
                        metrics.record_restart(&pipeline.id, &formatted_message).await;
                    }

                    let backoff = std::cmp::min(
                        pipeline.recovery.initial_backoff.mul_f32(
                            pipeline.recovery.backoff_multiplier.powi(attempts as i32 - 1)
                        ),
                        pipeline.recovery.max_backoff,
                    );

                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            publish_status(&inner, &pipeline.id, TaskStatus::Stopped, None).await;
                            break;
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }
                } else {
                    publish_status(&inner, &pipeline.id, TaskStatus::Failed, Some(message)).await;
                    break;
                }
            }
            PipelineLoopExit::Fatal(message) => {
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
    let buffer_pool = BufferPool::new(pipeline.runtime.buffer_size);
    buffer_pool.preallocate(pipeline.runtime.buffer_count);

    let mut context = PipelineContext {
        pipeline: pipeline.clone(),
        buffer_pool: buffer_pool.clone(),
        metrics: metrics.clone(),
    };

    let result = run_pipeline_loop(cancellation, &mut context, &mut stages, &metrics).await;

    if let Some(m) = &metrics {
        m.record_memory_watermark(&pipeline.id, buffer_pool.high_watermark_bytes()).await;
    }

    result
}

async fn run_pipeline_loop(
    cancellation: CancellationToken,
    context: &mut PipelineContext,
    stages: &mut PipelineStages,
    metrics: &Option<Arc<dyn crate::metrics::MetricsExporter>>,
) -> PipelineLoopExit {
    loop {
        let mut payload = tokio::select! {
            _ = cancellation.cancelled() => return PipelineLoopExit::Cancelled,
            result = timeout(context.pipeline.runtime.watchdog_timeout, stages.source.next(context)) => {
                match result {
                    Ok(Ok(payload)) => payload,
                    Ok(Err(error)) => {
                        if let Some(m) = metrics {
                            m.record_stream_error(&context.pipeline.id, &error.to_string()).await;
                        }
                        return pipeline_exit_from_error(error);
                    }
                    Err(_) => {
                        return PipelineLoopExit::Recoverable(format!(
                            "no media received within {:?}",
                            context.pipeline.runtime.watchdog_timeout,
                        ));
                    }
                }
            }
        };

        if let Some(m) = metrics {
            if let Some(data) = payload.data() {
                m.record_throughput(&context.pipeline.id, data.len()).await;
            }
        }

        if matches!(payload, MediaPayload::EndOfStream) {
            return PipelineLoopExit::EndOfStream;
        }

        for transform in &mut stages.transforms {
            match transform.transform(payload, context).await {
                Ok(transformed) => payload = transformed,
                Err(error) => {
                    if let Some(m) = metrics {
                        m.record_stream_error(&context.pipeline.id, &error.to_string())
                            .await;
                    }
                    return pipeline_exit_from_error(error);
                }
            }
        }

        if let Err(error) = stages.sink.consume(payload, context).await {
            if let Some(m) = metrics {
                m.record_stream_error(&context.pipeline.id, &error.to_string())
                    .await;
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
    let _ = inner.events_tx.send(RuntimeEvent::StatusChanged {
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
        MediaSource, MediaStorage, PipelineContext, RuntimeError, SinkFactory, SourceFactory,
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
            Self {
                actions: std::sync::Arc::new(std::sync::Mutex::new(VecDeque::from(actions))),
            }
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
                }
                .ok_or_else(|| {
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
                            data: MediaStorage::Pooled(data),
                        }));
                    }
                    MockSourceAction::Sleep(duration) => tokio::time::sleep(duration).await,
                    MockSourceAction::Stall => {
                        pending::<()>().await;
                        unreachable!("pending future never resolves")
                    }
                    MockSourceAction::Fail(message) => {
                        return Err(RuntimeError::recoverable(message))
                    }
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
