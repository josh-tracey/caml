#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use std::sync::Arc;

use async_trait::async_trait;
use caml_core::{
    CompiledPipeline, MediaSink, MediaSource, PipelineFactory, PipelineStages, RuntimeError,
};

#[derive(Default, Clone)]
pub struct BuiltinAdapters {
    #[cfg(feature = "ffmpeg")]
    pub ffmpeg_source: Option<Arc<caml_ffmpeg::FfmpegSourceFactory>>,
    #[cfg(feature = "pi")]
    pub libcamera_source: Option<Arc<caml_linux_media::LibcameraSourceFactory>>,
    #[cfg(feature = "webrtc")]
    pub webrtc_sinks: HashMap<String, Arc<caml_webrtc::WebRtcSinkFactory>>,
    pub recording_packets: Option<Arc<tokio::sync::Mutex<Vec<caml_core::runtime::RecordedPacket>>>>,
}

#[async_trait]
impl PipelineFactory for BuiltinAdapters {
    async fn build_pipeline(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<PipelineStages, RuntimeError> {
        let source_res: Result<Box<dyn MediaSource>, RuntimeError> = match pipeline.resolved_backend
        {
            caml_core::ResolvedInputBackend::FfmpegRtsp
            | caml_core::ResolvedInputBackend::AutoDevice
            | caml_core::ResolvedInputBackend::V4l2Device => {
                #[cfg(feature = "ffmpeg")]
                {
                    if let Some(factory) = &self.ffmpeg_source {
                        caml_core::SourceFactory::build_source(factory.as_ref(), pipeline).await
                    } else {
                        Err(RuntimeError::adapter(
                            "ffmpeg source factory not configured",
                        ))
                    }
                }
                #[cfg(not(feature = "ffmpeg"))]
                {
                    Err(RuntimeError::adapter("ffmpeg feature is disabled"))
                }
            }
            caml_core::ResolvedInputBackend::LibcameraDevice => {
                #[cfg(feature = "pi")]
                {
                    if let Some(factory) = &self.libcamera_source {
                        caml_core::SourceFactory::build_source(factory.as_ref(), pipeline).await
                    } else {
                        Err(RuntimeError::adapter(
                            "libcamera provider factory not configured",
                        ))
                    }
                }
                #[cfg(not(feature = "pi"))]
                {
                    Err(RuntimeError::adapter("pi feature is disabled"))
                }
            }
        };
        let source = source_res?;

        let has_webrtc_output = pipeline
            .outputs
            .iter()
            .any(|o| matches!(o, caml_core::OutputProfile::WebrtcRtp { .. }));

        let has_recording_output = pipeline
            .outputs
            .iter()
            .any(|o| matches!(o, caml_core::OutputProfile::Recording));

        let sink_res: Result<Box<dyn MediaSink>, RuntimeError> = if has_recording_output {
            let packets = self.recording_packets.clone().unwrap_or_else(|| {
                Arc::new(tokio::sync::Mutex::new(Vec::new()))
            });
            Ok(Box::new(caml_core::runtime::RecordingSink { packets }))
        } else if has_webrtc_output {
            #[cfg(feature = "webrtc")]
            {
                if let Some(factory) = self.webrtc_sinks.get(&pipeline.id) {
                    caml_core::SinkFactory::build_sink(factory.as_ref(), pipeline).await
                } else {
                    Err(RuntimeError::adapter(format!(
                        "no webrtc sink configured for pipeline {}",
                        pipeline.id
                    )))
                }
            }
            #[cfg(not(feature = "webrtc"))]
            {
                Err(RuntimeError::adapter("webrtc feature is disabled"))
            }
        } else {
            Ok(Box::new(NullSink))
        };
        let sink = sink_res?;

        Ok(PipelineStages {
            source,
            transforms: Vec::new(),
            sink,
        })
    }
}

impl From<BuiltinAdapters> for caml_core::RuntimeFactory {
    fn from(adapters: BuiltinAdapters) -> Self {
        caml_core::RuntimeFactory::new(Arc::new(adapters))
    }
}

struct NullSink;

#[async_trait]
impl MediaSink for NullSink {
    async fn consume(
        &mut self,
        _payload: caml_core::MediaPayload,
        _context: &mut caml_core::PipelineContext,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}
