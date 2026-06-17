use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use caml_core::{
    metrics::MetricsExporter, CompiledPipeline, EncodedPacket, HostCapabilities, MediaPayload,
    MediaSink, PipelineContext, RuntimeError, SinkFactory, StaticCapabilityProbe,
};
use rtp::{
    codecs::h264::H264Payloader,
    packet::Packet,
    packetizer::{new_packetizer, Packetizer},
    sequence::new_random_sequencer,
};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
pub use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

const DEFAULT_PACKET_SIZE_LIMIT: usize = 1200;
const DEFAULT_QUEUE_LIMIT: usize = 10;
const H264_PAYLOAD_TYPE: u8 = 102;
const VIDEO_CLOCK_RATE: u32 = 90_000;
const DEFAULT_FRAME_DURATION: Duration = Duration::from_millis(33);
const MIN_LATENESS_BUDGET: Duration = Duration::from_millis(50);

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
        let webrtc_output = pipeline.outputs.iter().find(|profile| {
            matches!(
                profile,
                caml_core::frontend::OutputProfile::WebrtcRtp { .. }
            )
        });

        let (
            codec_opt,
            payload_type_opt,
            mtu_opt,
            ssrc_opt,
            clock_rate_opt,
            queue_limit_opt,
            drop_policy,
        ) = match webrtc_output {
            Some(caml_core::frontend::OutputProfile::WebrtcRtp {
                codec,
                payload_type,
                mtu,
                ssrc,
                clock_rate,
                queue_limit,
                drop_policy,
            }) => (
                codec.clone(),
                *payload_type,
                *mtu,
                ssrc.clone(),
                *clock_rate,
                *queue_limit,
                *drop_policy,
            ),
            _ => (
                None,
                None,
                None,
                None,
                None,
                None,
                caml_core::frontend::DropPolicy::Block,
            ),
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

        let queue_limit = queue_limit_opt.unwrap_or(DEFAULT_QUEUE_LIMIT);
        if queue_limit == 0 {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' queue_limit must be at least 1 for WebRTC output",
                pipeline.id
            )));
        }

        Ok(Box::new(WebRtcSink::new_configured(
            pipeline.id.clone(),
            Arc::clone(&self.writer),
            final_mtu,
            default_frame_duration,
            codec,
            payload_type,
            ssrc,
            clock_rate,
            queue_limit,
            frontend_to_runtime_drop_policy(drop_policy),
        )?))
    }
}

struct WebRtcSink {
    pipeline_id: String,
    queue_limit: usize,
    drop_policy: caml_core::DropPolicy,
    shared: Arc<Mutex<WebRtcSharedState>>,
    notify: Arc<Notify>,
    sender_task: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct WebRtcSharedState {
    queue: VecDeque<EncodedPacket>,
    closed: bool,
    error: Option<RuntimeError>,
    metrics: Option<Arc<dyn MetricsExporter>>,
}

struct WebRtcSenderConfig {
    pipeline_id: String,
    writer: Arc<dyn RtpPacketWriter>,
    packet_size_limit: usize,
    default_frame_duration: Option<Duration>,
    codec: String,
    payload_type: u8,
    ssrc: u32,
    clock_rate: u32,
    lateness_budget: Duration,
}

#[derive(Default)]
struct SenderClock {
    base_pts: Option<Duration>,
    playback_start: Option<Instant>,
    last_released_timestamp: Option<Duration>,
    fallback_deadline: Option<Instant>,
}

struct DequeuedPacket {
    packet: EncodedPacket,
    newer_keyframe_queued: bool,
    metrics: Option<Arc<dyn MetricsExporter>>,
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
        Self::new_configured(
            pipeline_id,
            writer,
            packet_size_limit,
            default_frame_duration,
            "h264".to_string(),
            H264_PAYLOAD_TYPE,
            ssrc,
            VIDEO_CLOCK_RATE,
            DEFAULT_QUEUE_LIMIT,
            caml_core::DropPolicy::Block,
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
        queue_limit: usize,
        drop_policy: caml_core::DropPolicy,
    ) -> Result<Self, RuntimeError> {
        if packet_size_limit <= 12 {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' packet_size_limit {} is too small for RTP output",
                pipeline_id, packet_size_limit
            )));
        }

        if queue_limit == 0 {
            return Err(RuntimeError::sink(format!(
                "pipeline '{}' queue_limit must be at least 1 for WebRTC output",
                pipeline_id
            )));
        }

        let notify = Arc::new(Notify::new());
        let shared = Arc::new(Mutex::new(WebRtcSharedState {
            queue: VecDeque::with_capacity(queue_limit),
            closed: false,
            error: None,
            metrics: None,
        }));
        let sender_config = WebRtcSenderConfig {
            pipeline_id: pipeline_id.clone(),
            writer,
            packet_size_limit,
            default_frame_duration,
            codec,
            payload_type,
            ssrc,
            clock_rate,
            lateness_budget: lateness_budget(default_frame_duration),
        };
        let sender_task = tokio::spawn(run_sender_loop(
            Arc::clone(&shared),
            Arc::clone(&notify),
            sender_config,
        ));

        Ok(Self {
            pipeline_id,
            queue_limit,
            drop_policy,
            shared,
            notify,
            sender_task: Some(sender_task),
        })
    }

    async fn shared_error(&self) -> Result<(), RuntimeError> {
        let error = { self.shared.lock().await.error.clone() };
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn install_metrics(&self, metrics: Option<Arc<dyn MetricsExporter>>) {
        if let Some(metrics) = metrics {
            let mut shared = self.shared.lock().await;
            if shared.metrics.is_none() {
                shared.metrics = Some(metrics);
            }
        }
    }

    async fn record_backpressure_drop(&self, metrics: Option<Arc<dyn MetricsExporter>>) {
        if let Some(metrics) = metrics {
            metrics.record_backpressure_drop(&self.pipeline_id).await;
        }
    }

    async fn enqueue_packet(
        &mut self,
        packet: EncodedPacket,
        metrics: Option<Arc<dyn MetricsExporter>>,
    ) -> Result<(), RuntimeError> {
        loop {
            self.shared_error().await?;

            let maybe_wait = {
                let mut shared = self.shared.lock().await;
                if shared.closed {
                    return Err(RuntimeError::sink(format!(
                        "pipeline '{}' cannot enqueue after WebRTC sink is closed",
                        self.pipeline_id
                    )));
                }

                if shared.queue.len() < self.queue_limit {
                    shared.queue.push_back(packet);
                    self.notify.notify_one();
                    return Ok(());
                }

                match self.drop_policy {
                    caml_core::DropPolicy::Block => Some(self.notify.notified()),
                    caml_core::DropPolicy::DropNewest => {
                        drop(shared);
                        self.record_backpressure_drop(metrics.clone()).await;
                        return Ok(());
                    }
                    caml_core::DropPolicy::DropOldest => {
                        drop_oldest_prefer_delta(&mut shared.queue);
                        shared.queue.push_back(packet);
                        self.notify.notify_one();
                        drop(shared);
                        self.record_backpressure_drop(metrics.clone()).await;
                        return Ok(());
                    }
                }
            };

            if let Some(wait_for_capacity) = maybe_wait {
                wait_for_capacity.await;
            }
        }
    }

    async fn close_and_flush(&mut self) -> Result<(), RuntimeError> {
        {
            let mut shared = self.shared.lock().await;
            shared.closed = true;
        }
        self.notify.notify_waiters();

        if let Some(task) = self.sender_task.take() {
            let _ = task.await;
        }

        self.shared_error().await
    }
}

#[async_trait]
impl MediaSink for WebRtcSink {
    async fn consume(
        &mut self,
        payload: MediaPayload,
        context: &mut PipelineContext,
    ) -> Result<(), RuntimeError> {
        self.install_metrics(context.metrics.clone()).await;
        self.shared_error().await?;

        match payload {
            MediaPayload::EncodedPacket(packet) => {
                self.enqueue_packet(packet, context.metrics.clone()).await
            }
            MediaPayload::DecodedFrame(_) => Err(RuntimeError::sink(format!(
                "pipeline '{}' produced decoded frames but the current WebRTC sink expects packetized encoded video",
                self.pipeline_id
            ))),
            MediaPayload::EndOfStream => self.close_and_flush().await,
        }
    }
}

impl Drop for WebRtcSink {
    fn drop(&mut self) {
        if let Some(task) = self.sender_task.take() {
            task.abort();
        }
    }
}

async fn run_sender_loop(
    shared: Arc<Mutex<WebRtcSharedState>>,
    notify: Arc<Notify>,
    config: WebRtcSenderConfig,
) {
    let mut packetizer = Box::new(new_packetizer(
        config.packet_size_limit,
        config.payload_type,
        config.ssrc,
        Box::new(H264Payloader::default()),
        Box::new(new_random_sequencer()),
        config.clock_rate,
    )) as Box<dyn Packetizer + Send + Sync>;
    let mut clock = SenderClock::default();

    loop {
        let next = dequeue_packet(Arc::clone(&shared), Arc::clone(&notify)).await;
        let Some(next) = next else {
            return;
        };

        if let Err(error) = process_packet(&config, &mut *packetizer, &mut clock, next).await {
            set_sender_error(&shared, &notify, error).await;
            return;
        }
    }
}

async fn dequeue_packet(
    shared: Arc<Mutex<WebRtcSharedState>>,
    notify: Arc<Notify>,
) -> Option<DequeuedPacket> {
    loop {
        let maybe_wait = {
            let mut shared = shared.lock().await;

            if shared.error.is_some() {
                return None;
            }

            if let Some(packet) = shared.queue.pop_front() {
                let newer_keyframe_queued = shared.queue.iter().any(|queued| queued.is_keyframe);
                let metrics = shared.metrics.clone();
                notify.notify_waiters();
                return Some(DequeuedPacket {
                    packet,
                    newer_keyframe_queued,
                    metrics,
                });
            }

            if shared.closed {
                notify.notify_waiters();
                return None;
            }

            notify.notified()
        };

        maybe_wait.await;
    }
}

async fn process_packet(
    config: &WebRtcSenderConfig,
    packetizer: &mut (dyn Packetizer + Send + Sync),
    clock: &mut SenderClock,
    next: DequeuedPacket,
) -> Result<(), RuntimeError> {
    let packet = next.packet;
    if !packet.codec.eq_ignore_ascii_case(&config.codec) {
        let only_codec = if config.codec.eq_ignore_ascii_case("h264") {
            "H.264".to_string()
        } else {
            config.codec.clone()
        };
        return Err(RuntimeError::sink(format!(
            "pipeline '{}' emitted codec '{}' but the current WebRTC sink only packetizes {}",
            config.pipeline_id, packet.codec, only_codec
        )));
    }

    let frame_duration = packet
        .duration
        .or(config.default_frame_duration)
        .unwrap_or(DEFAULT_FRAME_DURATION);

    if should_drop_for_timestamp(clock, packet.timestamp) {
        return Ok(());
    }

    if should_drop_for_lateness(
        clock,
        packet.timestamp,
        frame_duration,
        config.lateness_budget,
        packet.is_keyframe,
        next.newer_keyframe_queued,
    )
    .await
    {
        if let Some(timestamp) = packet.timestamp {
            clock.last_released_timestamp = Some(timestamp);
        }
        return Ok(());
    }

    if let Some(metrics) = &next.metrics {
        metrics
            .record_copy_event(
                &config.pipeline_id,
                caml_core::metrics::CopyEvent::WebRtcPacketizerCopy,
                packet.data.len(),
            )
            .await;
    }

    let samples = duration_to_rtp_samples_with_rate(frame_duration, config.clock_rate);
    let packets = packetizer
        .packetize(&Bytes::copy_from_slice(packet.data.as_slice()), samples)
        .map_err(|error| {
            RuntimeError::sink(format!(
                "pipeline '{}' failed to packetize H.264 RTP payloads: {}",
                config.pipeline_id, error
            ))
        })?;

    for rtp_packet in packets {
        config.writer.write_packet(&rtp_packet).await?;
    }

    if let Some(timestamp) = packet.timestamp {
        clock.last_released_timestamp = Some(timestamp);
    }

    Ok(())
}

fn should_drop_for_timestamp(clock: &SenderClock, timestamp: Option<Duration>) -> bool {
    match (clock.last_released_timestamp, timestamp) {
        (Some(last), Some(current)) => current <= last,
        _ => false,
    }
}

async fn should_drop_for_lateness(
    clock: &mut SenderClock,
    timestamp: Option<Duration>,
    frame_duration: Duration,
    lateness_budget: Duration,
    is_keyframe: bool,
    newer_keyframe_queued: bool,
) -> bool {
    if let Some(timestamp) = timestamp {
        let base_pts = *clock.base_pts.get_or_insert(timestamp);
        let playback_start = *clock.playback_start.get_or_insert_with(Instant::now);
        let scheduled_send = playback_start + timestamp.saturating_sub(base_pts);
        let now = Instant::now();

        if scheduled_send > now {
            tokio::time::sleep(scheduled_send.duration_since(now)).await;
            clock.fallback_deadline = Some(Instant::now() + frame_duration);
            return false;
        }

        let lateness = now.duration_since(scheduled_send);
        clock.fallback_deadline = Some(now + frame_duration);
        if lateness > lateness_budget && (!is_keyframe || newer_keyframe_queued) {
            return true;
        }

        return false;
    }

    let now = Instant::now();
    let deadline = clock.fallback_deadline.get_or_insert(now);
    if *deadline > now {
        tokio::time::sleep(*deadline - now).await;
    }
    *deadline = Instant::now() + frame_duration;
    false
}

async fn set_sender_error(
    shared: &Arc<Mutex<WebRtcSharedState>>,
    notify: &Arc<Notify>,
    error: RuntimeError,
) {
    let mut shared = shared.lock().await;
    if shared.error.is_none() {
        shared.error = Some(error);
    }
    drop(shared);
    notify.notify_waiters();
}

fn drop_oldest_prefer_delta(queue: &mut VecDeque<EncodedPacket>) -> Option<EncodedPacket> {
    if let Some(index) = queue.iter().position(|packet| !packet.is_keyframe) {
        return queue.remove(index);
    }

    queue.pop_front()
}

fn frontend_to_runtime_drop_policy(
    policy: caml_core::frontend::DropPolicy,
) -> caml_core::DropPolicy {
    match policy {
        caml_core::frontend::DropPolicy::Block => caml_core::DropPolicy::Block,
        caml_core::frontend::DropPolicy::DropOldest => caml_core::DropPolicy::DropOldest,
        caml_core::frontend::DropPolicy::DropNewest => caml_core::DropPolicy::DropNewest,
    }
}

fn lateness_budget(default_frame_duration: Option<Duration>) -> Duration {
    let baseline = default_frame_duration.unwrap_or(DEFAULT_FRAME_DURATION);
    baseline
        .checked_mul(2)
        .unwrap_or(Duration::MAX)
        .max(MIN_LATENESS_BUDGET)
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
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use caml_core::{
        CodecPath, CompiledInput, CompiledPipeline, EncodedPacket, ExecutionMode, InputType,
        MediaPayload, MediaStorage, PipelineContext, RecoveryClass, RecoveryPolicy,
        ResolvedInputBackend, RuntimePolicy, SinkFactory, StreamStrategy,
    };
    use rtp::packet::Packet;
    use tokio::sync::{Mutex, Notify};

    use super::{
        drop_oldest_prefer_delta, duration_to_rtp_samples, RtpPacketWriter, WebRtcSinkFactory,
    };

    #[derive(Default)]
    struct RecorderWriter {
        packets: Mutex<VecDeque<Packet>>,
        instants: Mutex<Vec<Instant>>,
    }

    #[async_trait]
    impl RtpPacketWriter for RecorderWriter {
        async fn write_packet(&self, packet: &Packet) -> Result<(), caml_core::RuntimeError> {
            self.instants.lock().await.push(Instant::now());
            self.packets.lock().await.push_back(packet.clone());
            Ok(())
        }
    }

    struct BlockingWriter {
        started: Arc<AtomicUsize>,
        released: Arc<std::sync::atomic::AtomicBool>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl RtpPacketWriter for BlockingWriter {
        async fn write_packet(&self, _packet: &Packet) -> Result<(), caml_core::RuntimeError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            while !self.released.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            Ok(())
        }
    }

    fn build_pipeline(
        queue_limit: Option<usize>,
        drop_policy: caml_core::frontend::DropPolicy,
    ) -> CompiledPipeline {
        CompiledPipeline {
            id: "camera_a".to_string(),
            input: CompiledInput {
                kind: InputType::Rtsp,
                source: "rtsp://127.0.0.1:8554/live".to_string(),
            },
            strategy: StreamStrategy::Passthrough,
            network: None,
            processing: None,
            overlay: None,
            runtime: RuntimePolicy {
                buffer_size: 1024,
                watchdog_timeout: Duration::from_secs(5),
                buffer_count: 100,
            },
            resolved_backend: ResolvedInputBackend::FfmpegRtsp,
            execution_mode: ExecutionMode::EncodedPackets,
            codec_path: CodecPath::Passthrough,
            recovery: RecoveryPolicy {
                class: RecoveryClass::Network,
                max_restarts: 5,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                backoff_multiplier: 2.0,
                reset_after: Duration::from_secs(30),
            },
            capability_requirements: vec![],
            outputs: vec![caml_core::frontend::OutputProfile::WebrtcRtp {
                codec: Some("h264".to_string()),
                payload_type: Some(123),
                mtu: Some(1400),
                ssrc: Some("9999".to_string()),
                clock_rate: Some(90000),
                queue_limit,
                drop_policy,
            }],
        }
    }

    fn make_context(pipeline: &CompiledPipeline) -> PipelineContext {
        PipelineContext {
            pipeline: pipeline.clone(),
            buffer_pool: caml_core::runtime::BufferPool::new(1024),
            metrics: None,
        }
    }

    fn encoded_packet(
        timestamp: Option<Duration>,
        duration: Option<Duration>,
        is_keyframe: bool,
        byte: u8,
    ) -> EncodedPacket {
        EncodedPacket {
            codec: "h264".to_string(),
            timestamp,
            duration,
            is_keyframe,
            data: MediaStorage::from_vec(vec![
                0x00, 0x00, 0x00, 0x01, 0x65, byte, 0x84, 0x21, 0xA0, 0x10, 0xFF, 0xEE,
            ]),
        }
    }

    #[tokio::test]
    async fn packetizes_annex_b_h264_into_rtp_packets() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                Some(Duration::from_millis(40)),
                true,
                0x10,
            )),
            &mut context,
        )
        .await
        .expect("packetization should succeed");
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .expect("flush should succeed");

        let packets = writer.packets.lock().await;
        assert!(!packets.is_empty());
        assert_eq!(packets.front().unwrap().header.payload_type, 123);
        assert_eq!(packets.front().unwrap().header.ssrc, 9999);
        assert!(packets.back().unwrap().header.marker);
    }

    #[tokio::test]
    async fn rejects_non_h264_encoded_packets() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer);
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(EncodedPacket {
                codec: "h265".to_string(),
                ..encoded_packet(Some(Duration::from_millis(0)), None, true, 0x10)
            }),
            &mut context,
        )
        .await
        .expect("ingress queue should accept the packet");

        let error = sink
            .consume(MediaPayload::EndOfStream, &mut context)
            .await
            .expect_err("codec mismatch should surface at flush");
        assert!(error.to_string().contains("only packetizes H.264"));
    }

    #[test]
    fn converts_duration_to_rtp_samples() {
        assert_eq!(duration_to_rtp_samples(Duration::from_millis(40)), 3_600);
    }

    #[tokio::test]
    async fn test_custom_config_propagation() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("build_sink failed");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                Some(Duration::from_millis(40)),
                true,
                0x11,
            )),
            &mut context,
        )
        .await
        .expect("consume failed");
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .expect("flush should succeed");

        let packets = writer.packets.lock().await;
        assert!(!packets.is_empty());
        let packet = packets.front().unwrap();
        assert_eq!(packet.header.ssrc, 9999);
        assert_eq!(packet.header.payload_type, 123);
    }

    #[tokio::test]
    async fn paces_in_order_frames_from_timestamps() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                Some(Duration::from_millis(60)),
                true,
                0x20,
            )),
            &mut context,
        )
        .await
        .unwrap();
        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(60)),
                Some(Duration::from_millis(60)),
                false,
                0x21,
            )),
            &mut context,
        )
        .await
        .unwrap();
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .unwrap();

        let instants = writer.instants.lock().await.clone();
        assert!(instants.len() >= 2);
        let spacing = instants[1].duration_since(instants[0]);
        assert!(
            spacing >= Duration::from_millis(40),
            "expected paced spacing, got {:?}",
            spacing
        );
    }

    #[tokio::test]
    async fn drops_regressing_timestamps() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(60)),
                Some(Duration::from_millis(33)),
                true,
                0x30,
            )),
            &mut context,
        )
        .await
        .unwrap();
        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(30)),
                Some(Duration::from_millis(33)),
                false,
                0x31,
            )),
            &mut context,
        )
        .await
        .unwrap();
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .unwrap();

        let packets = writer.packets.lock().await;
        assert_eq!(packets.len(), 1);
    }

    #[tokio::test]
    async fn drops_overly_late_delta_frames_before_packetization() {
        let writer = Arc::new(RecorderWriter::default());
        let factory = WebRtcSinkFactory::from_writer(writer.clone());
        let pipeline = build_pipeline(None, caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                Some(Duration::from_millis(33)),
                true,
                0x40,
            )),
            &mut context,
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(130)).await;

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(10)),
                Some(Duration::from_millis(33)),
                false,
                0x41,
            )),
            &mut context,
        )
        .await
        .unwrap();
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .unwrap();

        let packets = writer.packets.lock().await;
        assert_eq!(packets.len(), 1);
    }

    #[test]
    fn keyframes_survive_drop_oldest_preference() {
        let mut queue = VecDeque::new();
        queue.push_back(encoded_packet(
            Some(Duration::from_millis(0)),
            None,
            true,
            0x50,
        ));
        queue.push_back(encoded_packet(
            Some(Duration::from_millis(33)),
            None,
            false,
            0x51,
        ));
        queue.push_back(encoded_packet(
            Some(Duration::from_millis(66)),
            None,
            false,
            0x52,
        ));

        let dropped = drop_oldest_prefer_delta(&mut queue).expect("one packet should drop");
        assert!(!dropped.is_keyframe);
        assert!(queue.iter().any(|packet| packet.is_keyframe));
    }

    #[tokio::test]
    async fn drop_newest_single_sink_does_not_block_ingress() {
        let started = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let writer = Arc::new(BlockingWriter {
            started: started.clone(),
            released: released.clone(),
            release: release.clone(),
        });
        let factory = WebRtcSinkFactory::from_writer(writer);
        let pipeline = build_pipeline(Some(1), caml_core::frontend::DropPolicy::DropNewest);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                None,
                true,
                0x60,
            )),
            &mut context,
        )
        .await
        .unwrap();

        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(33)),
                None,
                false,
                0x61,
            )),
            &mut context,
        )
        .await
        .unwrap();

        tokio::time::timeout(
            Duration::from_millis(100),
            sink.consume(
                MediaPayload::EncodedPacket(encoded_packet(
                    Some(Duration::from_millis(66)),
                    None,
                    false,
                    0x62,
                )),
                &mut context,
            ),
        )
        .await
        .expect("drop_newest should not block")
        .unwrap();

        released.store(true, Ordering::SeqCst);
        release.notify_waiters();
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn block_single_sink_applies_backpressure() {
        let started = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let writer = Arc::new(BlockingWriter {
            started: started.clone(),
            released: released.clone(),
            release: release.clone(),
        });
        let factory = WebRtcSinkFactory::from_writer(writer);
        let pipeline = build_pipeline(Some(1), caml_core::frontend::DropPolicy::Block);
        let mut sink = factory
            .build_sink(&pipeline)
            .await
            .expect("sink should build");
        let mut context = make_context(&pipeline);

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(0)),
                None,
                true,
                0x70,
            )),
            &mut context,
        )
        .await
        .unwrap();

        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        sink.consume(
            MediaPayload::EncodedPacket(encoded_packet(
                Some(Duration::from_millis(33)),
                None,
                false,
                0x71,
            )),
            &mut context,
        )
        .await
        .unwrap();

        let third = tokio::spawn({
            let mut sink = sink;
            let mut context = context.clone();
            async move {
                let result = tokio::time::timeout(
                    Duration::from_millis(80),
                    sink.consume(
                        MediaPayload::EncodedPacket(encoded_packet(
                            Some(Duration::from_millis(66)),
                            None,
                            false,
                            0x72,
                        )),
                        &mut context,
                    ),
                )
                .await;
                released.store(true, Ordering::SeqCst);
                release.notify_waiters();
                (result, sink, context)
            }
        });

        let (result, mut sink, mut context) = third.await.unwrap();
        assert!(
            result.is_err(),
            "block policy should backpressure when full"
        );
        sink.consume(MediaPayload::EndOfStream, &mut context)
            .await
            .unwrap();
    }
}
