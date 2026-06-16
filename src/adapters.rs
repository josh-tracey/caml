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

        // Collect all configured sinks for this pipeline.
        let mut sink_configs: Vec<caml_core::SinkActorConfig> = Vec::new();

        for output in &pipeline.outputs {
            match output {
                caml_core::OutputProfile::Recording {
                    queue_limit,
                    drop_policy,
                } => {
                    let packets = self.recording_packets.clone().unwrap_or_else(|| {
                        Arc::new(tokio::sync::Mutex::new(Vec::new()))
                    });
                    let queue_limit = queue_limit.unwrap_or(100);
                    sink_configs.push(caml_core::SinkActorConfig {
                        sink: Box::new(caml_core::runtime::RecordingSink { packets }),
                        queue_limit,
                        drop_policy: frontend_to_runtime_drop_policy(*drop_policy),
                    });
                }
                #[allow(unused_variables)]
                caml_core::OutputProfile::WebrtcRtp {
                    queue_limit,
                    drop_policy,
                    ..
                } => {
                    #[cfg(feature = "webrtc")]
                    {
                        if let Some(factory) = self.webrtc_sinks.get(&pipeline.id) {
                            let webrtc_sink = caml_core::SinkFactory::build_sink(
                                factory.as_ref(),
                                pipeline,
                            )
                            .await?;
                            let queue_limit = queue_limit.unwrap_or(10);
                            sink_configs.push(caml_core::SinkActorConfig {
                                sink: webrtc_sink,
                                queue_limit,
                                drop_policy: frontend_to_runtime_drop_policy(*drop_policy),
                            });
                        } else {
                            return Err(RuntimeError::adapter(format!(
                                "no webrtc sink configured for pipeline {}",
                                pipeline.id
                            )));
                        }
                    }
                    #[cfg(not(feature = "webrtc"))]
                    {
                        return Err(RuntimeError::adapter("webrtc feature is disabled"));
                    }
                }
            }
        }

        // Wrap in a FanoutRouter if multiple sinks are configured, use the
        // single sink directly when there is only one to avoid unnecessary overhead,
        // and fall back to NullSink when no outputs are declared.
        let sink: Box<dyn MediaSink> = match sink_configs.len() {
            0 => Box::new(NullSink),
            1 => sink_configs.remove(0).sink,
            _ => Box::new(caml_core::FanoutRouter::new(sink_configs)),
        };

        Ok(PipelineStages {
            source,
            transforms: Vec::new(),
            sink,
        })
    }
}

/// Convert the frontend `DropPolicy` (from the manifest schema) to the
/// runtime `DropPolicy` used by `FanoutRouter`.
fn frontend_to_runtime_drop_policy(
    policy: caml_core::frontend::DropPolicy,
) -> caml_core::DropPolicy {
    match policy {
        caml_core::frontend::DropPolicy::Block => caml_core::DropPolicy::Block,
        caml_core::frontend::DropPolicy::DropOldest => caml_core::DropPolicy::DropOldest,
        caml_core::frontend::DropPolicy::DropNewest => caml_core::DropPolicy::DropNewest,
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
