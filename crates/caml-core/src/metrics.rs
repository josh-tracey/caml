use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyEvent {
    FfmpegPacketToPooledBuffer,
    LibcameraFrameToPooledBuffer,
    WebRtcPacketizerCopy,
}

#[async_trait]
pub trait MetricsExporter: Send + Sync {
    async fn record_restart(&self, pipeline_id: &str, reason: &str);
    async fn record_backpressure_drop(&self, pipeline_id: &str);
    async fn record_memory_watermark(&self, pipeline_id: &str, bytes: usize);
    async fn record_throughput(&self, pipeline_id: &str, bytes: usize);
    async fn record_stream_error(&self, pipeline_id: &str, error: &str);
    async fn record_copy_event(&self, pipeline_id: &str, event: CopyEvent, bytes: usize);
}

#[derive(Clone, Default)]
pub struct NoopMetricsExporter;

#[async_trait]
impl MetricsExporter for NoopMetricsExporter {
    async fn record_restart(&self, _pipeline_id: &str, _reason: &str) {}
    async fn record_backpressure_drop(&self, _pipeline_id: &str) {}
    async fn record_memory_watermark(&self, _pipeline_id: &str, _bytes: usize) {}
    async fn record_throughput(&self, _pipeline_id: &str, _bytes: usize) {}
    async fn record_stream_error(&self, _pipeline_id: &str, _error: &str) {}
    async fn record_copy_event(&self, _pipeline_id: &str, _event: CopyEvent, _bytes: usize) {}
}
