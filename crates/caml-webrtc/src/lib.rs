use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use caml_core::{
    CompiledPipeline, HostCapabilities, MediaPayload, MediaSink, PipelineContext, RuntimeError,
    SinkFactory, StaticCapabilityProbe,
};
use rtp::{
    codecs::h264::H264Payloader,
    packet::Packet,
    packetizer::{new_packetizer, Packetizer},
    sequence::new_random_sequencer,
};
pub use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

const DEFAULT_PACKET_SIZE_LIMIT: usize = 1200;
const H264_PAYLOAD_TYPE: u8 = 102;
const VIDEO_CLOCK_RATE: u32 = 90_000;
const DEFAULT_FRAME_DURATION: Duration = Duration::from_millis(33);

pub fn webrtc_capabilities() -> StaticCapabilityProbe {
    StaticCapabilityProbe::new(HostCapabilities {
        rtp_packetization_available: true,
        ..HostCapabilities::default()
    })
}

#[async_trait]
pub trait RtpPacketWriter: Send + Sync {
    async fn write_packet(&self, packet: &Packet) -> Result<(), RuntimeError>;
}

struct TrackPacketWriter {
    track: Arc<TrackLocalStaticRTP>,
}

#[async_trait]
impl RtpPacketWriter for TrackPacketWriter {
    async fn write_packet(&self, packet: &Packet) -> Result<(), RuntimeError> {
        tokio::time::timeout(
            Duration::from_millis(500),
            self.track.write_rtp_with_extensions(packet, &[]),
        )
        .await
        .map_err(|_| RuntimeError::sink("WebRTC track write stalled due to backpressure timeout"))?
        .map(|_| ())
        .map_err(|error| RuntimeError::sink(format!("failed to write RTP packet: {}", error)))
    }
}

#[derive(Clone)]
pub struct WebRtcSinkFactory {
    writer: Arc<dyn RtpPacketWriter>,
}

impl WebRtcSinkFactory {
    pub fn new(track: Arc<TrackLocalStaticRTP>) -> Self {
        Self {
            writer: Arc::new(TrackPacketWriter { track }),
        }
    }

    pub fn from_writer(writer: Arc<dyn RtpPacketWriter>) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl SinkFactory for WebRtcSinkFactory {
    async fn build_sink(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn MediaSink>, RuntimeError> {
        let webrtc_output = pipeline
            .outputs
            .iter()
            .find(|profile| matches!(profile, caml_core::frontend::OutputProfile::WebrtcRtp { .. }));

        let (codec_opt, payload_type_opt, mtu_opt, ssrc_opt, clock_rate_opt) = match webrtc_output {
            Some(caml_core::frontend::OutputProfile::WebrtcRtp {
                codec,
                payload_type,
                mtu,
                ssrc,
                clock_rate,
            }) => (
                codec.clone(),
                *payload_type,
                *mtu,
                ssrc.clone(),
                *clock_rate,
            ),
            _ => (None, None, None, None, None),
        };

        let codec = codec_opt.unwrap_or_else(|| "h264".to_string());
        let payload_type = payload_type_opt.unwrap_or(H264_PAYLOAD_TYPE);
        let clock_rate = clock_rate_opt.unwrap_or(VIDEO_CLOCK_RATE);

        let final_mtu = mtu_opt.unwrap_or_else(|| {
            pipeline
                .network
                .as_ref()
                .map(|profile| profile.packet_size_limit)
                .unwrap_or(DEFAULT_PACKET_SIZE_LIMIT)
        });

        let ssrc = if let Some(ref ssrc_str) = ssrc_opt {
            if ssrc_str == "stable" {
                stable_ssrc(&pipeline.id)
            } else if let Ok(parsed_ssrc) = ssrc_str.parse::<u32>() {
                parsed_ssrc
            } else {
                stable_ssrc(ssrc_str)
            }
        } else {
            stable_ssrc(&pipeline.id)
        };

        let default_frame_duration = pipeline.processing.as_ref().and_then(|profile| {
            if profile.frame_rate == 0 {
                None
            } else {
                Some(Duration::from_nanos(
                    1_000_000_000u64 / profile.frame_rate as u64,
                ))
            }
        });

        Ok(Box::new(WebRtcSink::new_configured(
            pipeline.id.clone(),
            Arc::clone(&self.writer),
            final_mtu,
            default_frame_duration,
            codec,
            payload_type,
            ssrc,
            clock_rate,
        )?))
    }
}

struct WebRtcSink {
    pipeline_id: String,
    writer: Arc<dyn RtpPacketWriter>,
    packetizer: Box<dyn Packetizer + Send + Sync>,
    default_frame_duration: Option<Duration>,
    codec: String,
    clock_rate: u32,
}

impl WebRtcSink {
    #[allow(dead_code)]
    fn new(
        pipeline_id: String,
        writer: Arc<dyn RtpPacketWriter>,
        packet_size_limit: usize,
        default_frame_duration: Option<Duration>,
    ) -> Result<Self, RuntimeError> {
        let ssrc = stable_ssrc(&pipeline_id);
        Self::with_packetizer(
            pipeline_id,
            writer,
            packet_size_limit,
            default_frame_duration,
            Box::new(new_random_sequencer()),
            ssrc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_configured(
        pipeline_id: String,
        writer: Arc<dyn RtpPacketWriter>,
        packet_size_limit: usize,
        default_frame_duration: Option<Duration>,
        codec: String,
        payload_type: u8,
        ssrc: u32,
        clock_rate: u32,
    ) -> Result<Self, RuntimeError> {
        if packet_size_limit <= 12 {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' packet_size_limit {} is too small for RTP output",
                pipeline_id, packet_size_limit
            )));
        }

        Ok(Self {
            pipeline_id,
            writer,
            packetizer: Box::new(new_packetizer(
                packet_size_limit,
                payload_type,
                ssrc,
                Box::new(H264Payloader::default()),
                Box::new(new_random_sequencer()),
                clock_rate,
            )),
            default_frame_duration,
            codec,
            clock_rate,
        })
    }

    #[allow(dead_code)]
    fn with_packetizer(
        pipeline_id: String,
        writer: Arc<dyn RtpPacketWriter>,
        packet_size_limit: usize,
        default_frame_duration: Option<Duration>,
        sequencer: Box<dyn rtp::sequence::Sequencer + Send + Sync>,
        ssrc: u32,
    ) -> Result<Self, RuntimeError> {
        if packet_size_limit <= 12 {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' packet_size_limit {} is too small for RTP output",
                pipeline_id, packet_size_limit
            )));
        }

        Ok(Self {
            pipeline_id,
            writer,
            packetizer: Box::new(new_packetizer(
                packet_size_limit,
                H264_PAYLOAD_TYPE,
                ssrc,
                Box::new(H264Payloader::default()),
                sequencer,
                VIDEO_CLOCK_RATE,
            )),
            default_frame_duration,
            codec: "h264".to_string(),
            clock_rate: VIDEO_CLOCK_RATE,
        })
    }

    async fn write_encoded_packet(
        &mut self,
        codec: &str,
        payload: &[u8],
        duration: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        if !codec.eq_ignore_ascii_case(&self.codec) {
            let only_codec = if self.codec.eq_ignore_ascii_case("h264") {
                "H.264".to_string()
            } else {
                self.codec.clone()
            };
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' emitted codec '{}' but the current WebRTC sink only packetizes {}",
                self.pipeline_id, codec, only_codec
            )));
        }

        let sample_duration = duration
            .or(self.default_frame_duration)
            .unwrap_or(DEFAULT_FRAME_DURATION);
        let samples = duration_to_rtp_samples_with_rate(sample_duration, self.clock_rate);
        let packets = self
            .packetizer
            .packetize(&Bytes::copy_from_slice(payload), samples)
            .map_err(|error| {
                RuntimeError::sink(format!(
                    "pipeline '{}' failed to packetize H.264 RTP payloads: {}",
                    self.pipeline_id, error
                ))
            })?;

        for packet in packets {
            self.writer.write_packet(&packet).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl MediaSink for WebRtcSink {
    async fn consume(
        &mut self,
        payload: MediaPayload,
        context: &mut PipelineContext,
    ) -> Result<(), RuntimeError> {
        match payload {
            MediaPayload::EncodedPacket(packet) => {
                if let Some(m) = &context.metrics {
                    m.record_copy_event(
                        &self.pipeline_id,
                        caml_core::metrics::CopyEvent::WebRtcPacketizerCopy,
                        packet.data.len(),
                    )
                    .await;
                }
                self.write_encoded_packet(&packet.codec, packet.data.as_slice(), packet.duration)
                    .await
            }
            MediaPayload::DecodedFrame(_) => Err(RuntimeError::sink(format!(
                "pipeline '{}' produced decoded frames but the current WebRTC sink expects packetized encoded video",
                self.pipeline_id
            ))),
            MediaPayload::EndOfStream => Ok(()),
        }
    }
}

fn duration_to_rtp_samples_with_rate(duration: Duration, clock_rate: u32) -> u32 {
    let nanos = duration.as_nanos();
    let samples = nanos
        .saturating_mul(u128::from(clock_rate))
        .checked_div(1_000_000_000)
        .unwrap_or(0);
    samples.clamp(1, u128::from(u32::MAX)) as u32
}

#[allow(dead_code)]
fn duration_to_rtp_samples(duration: Duration) -> u32 {
    duration_to_rtp_samples_with_rate(duration, VIDEO_CLOCK_RATE)
}

fn stable_ssrc(pipeline_id: &str) -> u32 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    pipeline_id.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use rtp::{packet::Packet, sequence::new_fixed_sequencer};

    use super::{duration_to_rtp_samples, RtpPacketWriter, WebRtcSink};

    #[derive(Default)]
    struct RecorderWriter {
        packets: tokio::sync::Mutex<VecDeque<Packet>>,
    }

    #[async_trait]
    impl RtpPacketWriter for RecorderWriter {
        async fn write_packet(&self, packet: &Packet) -> Result<(), caml_core::RuntimeError> {
            self.packets.lock().await.push_back(packet.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn packetizes_annex_b_h264_into_rtp_packets() {
        let writer = Arc::new(RecorderWriter::default());
        let mut sink = WebRtcSink::with_packetizer(
            "camera_a".to_string(),
            writer.clone(),
            64,
            Some(Duration::from_millis(40)),
            Box::new(new_fixed_sequencer(10)),
            0x1122_3344,
        )
        .expect("sink should build");

        sink.write_encoded_packet(
            "h264",
            &[
                0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, 0xA0, 0x10, 0xFF, 0xEE, 0xDD, 0xCC,
                0xBB, 0xAA,
            ],
            Some(Duration::from_millis(40)),
        )
        .await
        .expect("packetization should succeed");

        let packets = writer.packets.lock().await;
        assert!(!packets.is_empty());
        assert_eq!(packets.front().unwrap().header.sequence_number, 10);
        assert!(packets.back().unwrap().header.marker);
        assert!(packets
            .iter()
            .all(|packet| packet.header.ssrc == 0x1122_3344));
    }

    #[tokio::test]
    async fn rejects_non_h264_encoded_packets() {
        let writer = Arc::new(RecorderWriter::default());
        let mut sink = WebRtcSink::with_packetizer(
            "camera_a".to_string(),
            writer,
            1200,
            None,
            Box::new(new_fixed_sequencer(1)),
            7,
        )
        .expect("sink should build");

        let error = sink
            .write_encoded_packet("h265", &[1, 2, 3], Some(Duration::from_millis(33)))
            .await
            .expect_err("codec mismatch should fail");
        assert!(error.to_string().contains("only packetizes H.264"));
    }

    #[test]
    fn converts_duration_to_rtp_samples() {
        assert_eq!(duration_to_rtp_samples(Duration::from_millis(40)), 3_600);
    }

    #[tokio::test]
    async fn test_custom_config_propagation() {
        use caml_core::{CompiledPipeline, CompiledInput, InputType, StreamStrategy, ResolvedInputBackend, RuntimePolicy, RecoveryPolicy, ExecutionMode, CodecPath};
        use caml_core::frontend::OutputProfile;
        use super::WebRtcSinkFactory;
        use caml_core::SinkFactory;

        let pipeline = CompiledPipeline {
            id: "camera_a".to_string(),
            input: CompiledInput {
                kind: InputType::Rtsp,
                source: "rtsp://127.0.0.1:8554/live".to_string(),
            },
            strategy: StreamStrategy::Passthrough,
            network: None,
            processing: None,
            runtime: RuntimePolicy {
                buffer_size: 1024,
                watchdog_timeout: Duration::from_secs(5),
                buffer_count: 100,
            },
            resolved_backend: ResolvedInputBackend::FfmpegRtsp,
            execution_mode: ExecutionMode::EncodedPackets,
            codec_path: CodecPath::Passthrough,
            recovery: RecoveryPolicy {
                class: caml_core::RecoveryClass::Network,
                max_restarts: 5,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                backoff_multiplier: 2.0,
                reset_after: Duration::from_secs(30),
            },
            capability_requirements: vec![],
            outputs: vec![OutputProfile::WebrtcRtp {
                codec: Some("h264".to_string()),
                payload_type: Some(123),
                mtu: Some(1400),
                ssrc: Some("9999".to_string()),
                clock_rate: Some(90000),
            }],
        };

        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let sink_box = factory.build_sink(&pipeline).await.expect("build_sink failed");
        
        let mut sink = sink_box;
        let mut context = caml_core::PipelineContext {
            pipeline: pipeline.clone(),
            buffer_pool: caml_core::runtime::BufferPool::new(1024),
            metrics: None,
        };

        let mut data = context.acquire_buffer();
        data.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, 0xA0, 0x10, 0xFF, 0xEE, 0xDD, 0xCC,
            0xBB, 0xAA,
        ]);
        
        sink.consume(caml_core::MediaPayload::EncodedPacket(caml_core::EncodedPacket {
            codec: "h264".to_string(),
            timestamp: None,
            duration: Some(Duration::from_millis(40)),
            is_keyframe: true,
            data: caml_core::MediaStorage::Pooled(data),
        }), &mut context).await.expect("consume failed");

        let packets = writer.packets.lock().await;
        assert!(!packets.is_empty());
        let packet = packets.front().unwrap();
        assert_eq!(packet.header.ssrc, 9999);
        assert_eq!(packet.header.payload_type, 123);
    }
}
