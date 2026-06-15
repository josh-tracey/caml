use std::thread;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use caml_core::{CompiledPipeline, MediaSource, PipelineContext, RuntimeError, SourceFactory, MediaPayload};

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
                let mut data = context.acquire_buffer();
                data.extend_from_slice(&packet.data);
                Ok(MediaPayload::EncodedPacket(caml_core::EncodedPacket {
                    codec: packet.codec,
                    timestamp: packet.timestamp,
                    duration: packet.duration,
                    is_keyframe: packet.is_keyframe,
                    data: caml_core::MediaStorage::Pooled(data),
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
