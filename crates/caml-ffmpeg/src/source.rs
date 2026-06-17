use async_trait::async_trait;
use std::thread;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use caml_core::{
    CompiledPipeline, MediaPayload, MediaSource, PipelineContext, RuntimeError, SourceFactory,
};

use crate::worker::{run_worker, worker_spec_for_pipeline, WorkerMessage};

#[derive(Clone, Default)]
pub struct FfmpegSourceFactory;

impl FfmpegSourceFactory {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SourceFactory for FfmpegSourceFactory {
    async fn build_source(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSource>, RuntimeError> {
        if pipeline.input.source.starts_with("mock:") || pipeline.input.source.contains("mock") {
            let (tx, rx) = mpsc::channel(8);
            let cancel = CancellationToken::new();
            let cancel_clone = cancel.clone();
            tokio::spawn(async move {
                for i in 0..5 {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                            let packet = WorkerMessage::Packet(crate::worker::OwnedEncodedPacket {
                                codec: "h264".to_string(),
                                timestamp: Some(std::time::Duration::from_millis(i * 33)),
                                duration: Some(std::time::Duration::from_millis(33)),
                                is_keyframe: i == 0,
                                data: vec![0, 0, 0, 1, 0x65, i as u8, 0, 0, 0],
                            });
                            if tx.send(packet).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                let _ = tx.send(WorkerMessage::EndOfStream).await;
            });

            return Ok(Box::new(FfmpegSource {
                receiver: rx,
                cancel,
            }));
        }

        let spec = worker_spec_for_pipeline(pipeline)?;
        let (tx, rx) = mpsc::channel(8); // WORKER_QUEUE_DEPTH = 8
        let cancel = CancellationToken::new();

        let cancel_worker = cancel.clone();
        thread::Builder::new()
            .name(format!("caml-ffmpeg-{}", pipeline.id))
            .spawn(move || {
                if let Err(error) = run_worker(spec, tx, cancel_worker) {
                    let _ = error;
                }
            })
            .map_err(|error| {
                RuntimeError::adapter(format!(
                    "failed to spawn FFmpeg worker for '{}': {}",
                    pipeline.id, error
                ))
            })?;

        Ok(Box::new(FfmpegSource {
            receiver: rx,
            cancel,
        }))
    }
}

pub struct FfmpegSource {
    receiver: mpsc::Receiver<WorkerMessage>,
    cancel: CancellationToken,
}

impl Drop for FfmpegSource {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[async_trait]
impl MediaSource for FfmpegSource {
    async fn next(&mut self, context: &mut PipelineContext) -> Result<MediaPayload, RuntimeError> {
        match self.receiver.recv().await {
            Some(WorkerMessage::Packet(packet)) => {
                if let Some(m) = &context.metrics {
                    m.record_copy_event(
                        &context.pipeline.id,
                        caml_core::metrics::CopyEvent::FfmpegPacketToPooledBuffer,
                        packet.data.len(),
                    )
                    .await;
                }
                let mut data = context.acquire_buffer();
                data.extend_from_slice(&packet.data);
                Ok(MediaPayload::EncodedPacket(caml_core::EncodedPacket {
                    codec: packet.codec,
                    timestamp: packet.timestamp,
                    duration: packet.duration,
                    is_keyframe: packet.is_keyframe,
                    data: caml_core::MediaStorage::Pooled(data.freeze()),
                }))
            }
            Some(WorkerMessage::EndOfStream) | None => Ok(MediaPayload::EndOfStream),
            Some(WorkerMessage::RecoverableError(message)) => {
                Err(RuntimeError::recoverable(message))
            }
            Some(WorkerMessage::Error(message)) => Err(RuntimeError::source(message)),
        }
    }
}
