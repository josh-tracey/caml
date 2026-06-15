use std::collections::HashMap;
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
}

#[async_trait]
impl PipelineFactory for BuiltinAdapters {
    async fn build_pipeline(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<PipelineStages, RuntimeError> {
        let source: Box<dyn MediaSource> = match pipeline.resolved_backend {
            caml_core::ResolvedInputBackend::FfmpegRtsp
            | caml_core::ResolvedInputBackend::AutoDevice
            | caml_core::ResolvedInputBackend::V4l2Device => {
                #[cfg(feature = "ffmpeg")]
                {
                    if let Some(factory) = &self.ffmpeg_source {
                        caml_core::SourceFactory::build_source(factory.as_ref(), pipeline).await?
                    } else {
                        return Err(RuntimeError::adapter(
                            "ffmpeg source factory not configured",
                        ));
                    }
                }
                #[cfg(not(feature = "ffmpeg"))]
                {
                    return Err(RuntimeError::adapter("ffmpeg feature is disabled"));
                }
            }
            caml_core::ResolvedInputBackend::LibcameraDevice => {
                #[cfg(feature = "pi")]
                {
                    if let Some(factory) = &self.libcamera_source {
                        caml_core::SourceFactory::build_source(factory.as_ref(), pipeline).await?
                    } else {
                        return Err(RuntimeError::adapter(
                            "libcamera provider factory not configured",
                        ));
                    }
                }
                #[cfg(not(feature = "pi"))]
                {
                    return Err(RuntimeError::adapter("pi feature is disabled"));
                }
            }
        };

        let has_webrtc_output = pipeline
            .outputs
            .iter()
            .any(|o| matches!(o, caml_core::OutputProfile::WebrtcRtp { .. }));

        let sink: Box<dyn MediaSink> = if has_webrtc_output {
            #[cfg(feature = "webrtc")]
            {
                let factory = self.webrtc_sinks.get(&pipeline.id).ok_or_else(|| {
                    RuntimeError::adapter(format!(
                        "missing webrtc track for pipeline '{}'",
                        pipeline.id
                    ))
                })?;
                caml_core::SinkFactory::build_sink(factory.as_ref(), pipeline).await?
            }
            #[cfg(not(feature = "webrtc"))]
            {
                return Err(RuntimeError::adapter("webrtc feature is disabled"));
            }
        } else {
            Box::new(NullSink)
        };

        Ok(PipelineStages {
            source,
            transforms: Vec::new(),
            sink,
        })
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
