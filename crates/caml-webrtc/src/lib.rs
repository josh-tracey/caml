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
        self.track
            .write_rtp_with_extensions(packet, &[])
            .await
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
        Ok(Box::new(WebRtcSink::new(
            pipeline.id.clone(),
            Arc::clone(&self.writer),
            pipeline
                .network
                .as_ref()
                .map(|profile| profile.packet_size_limit)
                .unwrap_or(DEFAULT_PACKET_SIZE_LIMIT),
            pipeline.processing.as_ref().and_then(|profile| {
                if profile.frame_rate == 0 {
                    None
                } else {
                    Some(Duration::from_nanos(
                        1_000_000_000u64 / profile.frame_rate as u64,
                    ))
                }
            }),
        )?))
    }
}

struct WebRtcSink {
    pipeline_id: String,
    writer: Arc<dyn RtpPacketWriter>,
    packetizer: Box<dyn Packetizer + Send + Sync>,
    default_frame_duration: Option<Duration>,
}

impl WebRtcSink {
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
        })
    }

    async fn write_encoded_packet(
        &mut self,
        codec: &str,
        payload: &[u8],
        duration: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        if !codec.eq_ignore_ascii_case("h264") {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' emitted codec '{}' but the current WebRTC sink only packetizes H.264",
                self.pipeline_id, codec
            )));
        }

        let sample_duration = duration
            .or(self.default_frame_duration)
            .unwrap_or(DEFAULT_FRAME_DURATION);
        let samples = duration_to_rtp_samples(sample_duration);
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
        _context: &mut PipelineContext,
    ) -> Result<(), RuntimeError> {
        match payload {
            MediaPayload::EncodedPacket(packet) => {
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

fn duration_to_rtp_samples(duration: Duration) -> u32 {
    let nanos = duration.as_nanos();
    let samples = nanos
        .saturating_mul(u128::from(VIDEO_CLOCK_RATE))
        .checked_div(1_000_000_000)
        .unwrap_or(0);
    samples.clamp(1, u128::from(u32::MAX)) as u32
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
}
