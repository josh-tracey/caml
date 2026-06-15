use std::{ffi::CString, sync::OnceLock, thread, time::Duration};

use async_trait::async_trait;
use caml_core::{
    CodecPath, CompiledPipeline, CompiledProcessingProfile, HostCapabilities, InputType,
    MediaPayload, MediaSource, PipelineContext, ResolvedInputBackend, RuntimeError, SourceFactory,
    StaticCapabilityProbe, StreamStrategy, Transport,
};
use ffmpeg_next as ffmpeg;
use tokio::sync::mpsc;

const WORKER_QUEUE_DEPTH: usize = 8;
const DEFAULT_FRAME_DURATION: Duration = Duration::from_millis(33);
const H264_START_CODE: &[u8] = &[0x00, 0x00, 0x00, 0x01];

pub fn ffmpeg_capabilities() -> StaticCapabilityProbe {
    StaticCapabilityProbe::new(HostCapabilities {
        ffmpeg_available: init_ffmpeg(),
        rtp_packetization_available: false,
        ..HostCapabilities::default()
    })
}

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
        let (tx, rx) = mpsc::channel(WORKER_QUEUE_DEPTH);

        thread::Builder::new()
            .name(format!("caml-ffmpeg-{}", pipeline.id))
            .spawn(move || {
                if let Err(error) = run_worker(spec, tx) {
                    let _ = error;
                }
            })
            .map_err(|error| {
                RuntimeError::adapter(format!(
                    "failed to spawn FFmpeg worker for '{}': {}",
                    pipeline.id, error
                ))
            })?;

        Ok(Box::new(FfmpegSource { receiver: rx }))
    }
}

fn worker_spec_for_pipeline(pipeline: &CompiledPipeline) -> Result<WorkerSpec, RuntimeError> {
    let transport = pipeline
        .network
        .as_ref()
        .map(|profile| profile.transport)
        .unwrap_or(Transport::Tcp);

    let default_frame_duration = pipeline
        .processing
        .as_ref()
        .and_then(frame_duration_from_processing)
        .unwrap_or(DEFAULT_FRAME_DURATION);

    let input = match pipeline.input.kind {
        InputType::Rtsp => InputSpec::Rtsp { transport },
        InputType::Device => match pipeline.resolved_backend {
            ResolvedInputBackend::AutoDevice | ResolvedInputBackend::V4l2Device => {
                InputSpec::Device {
                    backend: pipeline.resolved_backend,
                    frame_rate: pipeline
                        .processing
                        .as_ref()
                        .map(|processing| processing.frame_rate)
                        .filter(|frame_rate| *frame_rate > 0),
                }
            }
            ResolvedInputBackend::LibcameraDevice => {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' selected a libcamera device backend, but the current FFmpeg adapter only implements RTSP and V4L2-backed device ingest",
                    pipeline.id
                )));
            }
            ResolvedInputBackend::FfmpegRtsp => {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' resolved an invalid FFmpeg RTSP backend for device input",
                    pipeline.id
                )));
            }
        },
    };

    let mode = match pipeline.strategy {
        StreamStrategy::Passthrough => WorkerMode::Passthrough {
            default_frame_duration,
        },
        StreamStrategy::Transcode => {
            let processing = pipeline.processing.clone().ok_or_else(|| {
                RuntimeError::adapter(format!(
                    "pipeline '{}' was compiled for transcode but has no processing profile",
                    pipeline.id
                ))
            })?;

            validate_transcode_support(pipeline, &processing)?;
            let backend = transcode_backend_for_pipeline(pipeline)?;

            WorkerMode::Transcode {
                processing,
                backend,
                default_frame_duration,
            }
        }
        StreamStrategy::HardwareDecode => {
            let processing = pipeline.processing.clone().ok_or_else(|| {
                RuntimeError::adapter(format!(
                    "pipeline '{}' was compiled for hardware decode but has no processing profile",
                    pipeline.id
                ))
            })?;

            validate_transcode_support(pipeline, &processing)?;
            let backend = transcode_backend_for_pipeline(pipeline)?;

            WorkerMode::Transcode {
                processing,
                backend,
                default_frame_duration,
            }
        }
    };

    Ok(WorkerSpec {
        pipeline_id: pipeline.id.clone(),
        input: pipeline.input.source.clone(),
        input_spec: input,
        mode,
    })
}

fn validate_transcode_support(
    pipeline: &CompiledPipeline,
    processing: &CompiledProcessingProfile,
) -> Result<(), RuntimeError> {
    let encoder = processing.encoder.trim().to_ascii_lowercase();
    match pipeline.codec_path {
        CodecPath::SoftwareTranscode | CodecPath::HardwareDecode => {
            if !matches!(encoder.as_str(), "software" | "libx264") {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' requested encoder '{}' but the current FFmpeg backend only implements software H.264 encode for this pipeline",
                    pipeline.id, processing.encoder
                )));
            }
        }
        CodecPath::HardwareTranscode => {
            if !matches!(encoder.as_str(), "hardware" | "v4l2m2m") {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' requested encoder '{}' but Raspberry Pi hardware transcode requires 'hardware' or 'v4l2m2m'",
                    pipeline.id, processing.encoder
                )));
            }
        }
        CodecPath::Passthrough => {
            return Err(RuntimeError::adapter(format!(
                "pipeline '{}' cannot request transcode support while compiled for passthrough",
                pipeline.id
            )));
        }
    }

    let codec = processing.codec.trim().to_ascii_lowercase();
    if codec != "h264" {
        return Err(RuntimeError::adapter(format!(
            "pipeline '{}' requested codec '{}' but the current FFmpeg backend only implements software H.264 transcode",
            pipeline.id, processing.codec
        )));
    }

    frame_duration_from_processing(processing).ok_or_else(|| {
        RuntimeError::adapter(format!(
            "pipeline '{}' must set a positive frame_rate for software transcode",
            pipeline.id
        ))
    })?;

    normalize_rotation(processing.rotation).map_err(RuntimeError::adapter)?;

    Ok(())
}

fn transcode_backend_for_pipeline(
    pipeline: &CompiledPipeline,
) -> Result<TranscodeBackend, RuntimeError> {
    match pipeline.codec_path {
        CodecPath::SoftwareTranscode => Ok(TranscodeBackend::Software),
        CodecPath::HardwareTranscode => Ok(TranscodeBackend::Pi4HardwareEncode),
        CodecPath::HardwareDecode => Ok(TranscodeBackend::Pi5HardwareDecode),
        CodecPath::Passthrough => Err(RuntimeError::adapter(format!(
            "pipeline '{}' does not have a transcode codec path",
            pipeline.id
        ))),
    }
}

struct FfmpegSource {
    receiver: mpsc::Receiver<WorkerMessage>,
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
                    data,
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

#[derive(Debug)]
enum WorkerMessage {
    Packet(OwnedEncodedPacket),
    EndOfStream,
    RecoverableError(String),
    Error(String),
}

#[derive(Debug)]
struct OwnedEncodedPacket {
    codec: String,
    timestamp: Option<Duration>,
    duration: Option<Duration>,
    is_keyframe: bool,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct WorkerSpec {
    pipeline_id: String,
    input: String,
    input_spec: InputSpec,
    mode: WorkerMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InputSpec {
    Rtsp {
        transport: Transport,
    },
    Device {
        backend: ResolvedInputBackend,
        frame_rate: Option<u32>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TranscodeBackend {
    Software,
    Pi4HardwareEncode,
    Pi5HardwareDecode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkerMode {
    Passthrough {
        default_frame_duration: Duration,
    },
    Transcode {
        processing: CompiledProcessingProfile,
        backend: TranscodeBackend,
        default_frame_duration: Duration,
    },
}

fn init_ffmpeg() -> bool {
    static INIT_RESULT: OnceLock<bool> = OnceLock::new();
    *INIT_RESULT.get_or_init(|| ffmpeg::init().is_ok())
}

fn run_worker(spec: WorkerSpec, tx: mpsc::Sender<WorkerMessage>) -> Result<(), RuntimeError> {
    if !init_ffmpeg() {
        let _ = tx.blocking_send(WorkerMessage::Error(
            "ffmpeg initialization failed; verify FFmpeg libraries are available".to_string(),
        ));
        return Ok(());
    }

    let result = match &spec.mode {
        WorkerMode::Passthrough {
            default_frame_duration,
        } => demux_packets(&spec.input, &spec.input_spec, *default_frame_duration, &tx),
        WorkerMode::Transcode {
            processing,
            backend,
            default_frame_duration,
        } => transcode_packets(
            &spec.input,
            &spec.input_spec,
            processing,
            *backend,
            *default_frame_duration,
            &tx,
        ),
    };

    match result {
        Ok(()) => {
            let _ = tx.blocking_send(WorkerMessage::EndOfStream);
        }
        Err(error) => {
            let _ = tx.blocking_send(WorkerMessage::RecoverableError(format!(
                "pipeline '{}' failed during FFmpeg execution: {}",
                spec.pipeline_id, error
            )));
        }
    }

    Ok(())
}

fn demux_packets(
    input: &str,
    input_spec: &InputSpec,
    default_frame_duration: Duration,
    tx: &mpsc::Sender<WorkerMessage>,
) -> Result<(), String> {
    let mut context = open_media_input(input, input_spec)?;
    let (stream_index, time_base, codec_name, nominal_duration, h264_config) = {
        let stream = best_video_stream(&context, input)?;
        let codec_name = codec_name(stream.parameters().id())?;
        let h264_config = if codec_name == "h264" {
            extract_h264_config(&stream.parameters())?
        } else {
            None
        };

        (
            stream.index(),
            stream.time_base(),
            codec_name,
            frame_duration_from_rate(stream.avg_frame_rate()).unwrap_or(default_frame_duration),
            h264_config,
        )
    };

    for (stream, packet) in context.packets() {
        if stream.index() != stream_index {
            continue;
        }

        let raw_data = packet
            .data()
            .ok_or_else(|| "ffmpeg returned a packet with no payload".to_string())?;
        let normalized =
            normalize_encoded_packet(codec_name, raw_data, packet.is_key(), h264_config.as_ref())?;

        let message = WorkerMessage::Packet(OwnedEncodedPacket {
            codec: codec_name.to_string(),
            timestamp: duration_from_time_base(packet.pts(), time_base),
            duration: duration_from_packet(packet.duration(), time_base).or(Some(nominal_duration)),
            is_keyframe: packet.is_key(),
            data: normalized,
        });

        if tx.blocking_send(message).is_err() {
            break;
        }
    }

    Ok(())
}

fn transcode_packets(
    input: &str,
    input_spec: &InputSpec,
    processing: &CompiledProcessingProfile,
    backend: TranscodeBackend,
    default_frame_duration: Duration,
    tx: &mpsc::Sender<WorkerMessage>,
) -> Result<(), String> {
    let mut context = open_media_input(input, input_spec)?;
    let nominal_duration =
        frame_duration_from_processing(processing).unwrap_or(default_frame_duration);
    let (stream_index, mut transcoder) = {
        let stream = best_video_stream(&context, input)?;
        (
            stream.index(),
            VideoTranscoder::new(&stream, processing, backend, nominal_duration)?,
        )
    };

    for (stream, packet) in context.packets() {
        if stream.index() != stream_index {
            continue;
        }

        transcoder.send_packet_to_decoder(&packet)?;
        transcoder.receive_decoded_frames(tx)?;
    }

    transcoder.send_eof_to_decoder()?;
    transcoder.receive_decoded_frames(tx)?;
    transcoder.flush_filter()?;
    transcoder.receive_filtered_frames(tx)?;
    transcoder.send_eof_to_encoder()?;
    transcoder.receive_encoded_packets(tx)?;

    Ok(())
}

struct VideoTranscoder {
    decoder: ffmpeg::decoder::Video,
    filter: ffmpeg::filter::Graph,
    encoder: ffmpeg::encoder::Video,
    encoder_time_base: ffmpeg::Rational,
    encoder_h264_config: Option<H264Config>,
    nominal_duration: Duration,
}

impl VideoTranscoder {
    fn new(
        stream: &ffmpeg::format::stream::Stream<'_>,
        processing: &CompiledProcessingProfile,
        backend: TranscodeBackend,
        nominal_duration: Duration,
    ) -> Result<Self, String> {
        let decoder = open_video_decoder(stream, backend)?;

        let rotation = normalize_rotation(processing.rotation)?;
        let (output_width, output_height) =
            rotated_dimensions(decoder.width(), decoder.height(), rotation);
        let filter = build_video_filter(&decoder, stream.time_base(), rotation)?;
        let encoder =
            open_h264_encoder(&decoder, processing, backend, output_width, output_height)?;
        let encoder_time_base = encoder.time_base();
        let encoder_h264_config = extract_h264_config(&ffmpeg::codec::Parameters::from(&encoder))?;

        Ok(Self {
            decoder,
            filter,
            encoder,
            encoder_time_base,
            encoder_h264_config,
            nominal_duration,
        })
    }

    fn send_packet_to_decoder(&mut self, packet: &ffmpeg::Packet) -> Result<(), String> {
        self.decoder
            .send_packet(packet)
            .map_err(|error| format!("failed to send packet to decoder: {}", error))
    }

    fn send_eof_to_decoder(&mut self) -> Result<(), String> {
        self.decoder
            .send_eof()
            .map_err(|error| format!("failed to send decoder EOF: {}", error))
    }

    fn send_eof_to_encoder(&mut self) -> Result<(), String> {
        self.encoder
            .send_eof()
            .map_err(|error| format!("failed to send encoder EOF: {}", error))
    }

    fn receive_decoded_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut decoded = ffmpeg::frame::Video::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let pts = decoded.timestamp().or(decoded.pts());
            decoded.set_pts(pts);
            let mut source = self
                .filter
                .get("in")
                .ok_or_else(|| "filter graph is missing the input node".to_string())?;
            source
                .source()
                .add(&decoded)
                .map_err(|error| format!("failed to push frame into filter graph: {}", error))?;
            self.receive_filtered_frames(tx)?;
        }

        Ok(())
    }

    fn flush_filter(&mut self) -> Result<(), String> {
        let mut source = self
            .filter
            .get("in")
            .ok_or_else(|| "filter graph is missing the input node".to_string())?;
        source
            .source()
            .flush()
            .map_err(|error| format!("failed to flush video filter graph: {}", error))
    }

    fn receive_filtered_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut sink = self
            .filter
            .get("out")
            .ok_or_else(|| "filter graph is missing the output node".to_string())?;
        let mut filtered = ffmpeg::frame::Video::empty();

        while sink.sink().frame(&mut filtered).is_ok() {
            let pts = filtered.timestamp().or(filtered.pts());
            filtered.set_pts(pts);
            filtered.set_kind(ffmpeg::picture::Type::None);
            self.encoder
                .send_frame(&filtered)
                .map_err(|error| format!("failed to send frame to encoder: {}", error))?;
            self.receive_encoded_packets(tx)?;
        }

        Ok(())
    }

    fn receive_encoded_packets(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut encoded = ffmpeg::Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            let raw_data = encoded
                .data()
                .ok_or_else(|| "encoder returned a packet with no payload".to_string())?;
            let normalized = normalize_h264_payload(
                raw_data,
                encoded.is_key(),
                self.encoder_h264_config.as_ref(),
            )?;

            let message = WorkerMessage::Packet(OwnedEncodedPacket {
                codec: "h264".to_string(),
                timestamp: duration_from_time_base(encoded.pts(), self.encoder_time_base),
                duration: duration_from_packet(encoded.duration(), self.encoder_time_base)
                    .or(Some(self.nominal_duration)),
                is_keyframe: encoded.is_key(),
                data: normalized,
            });

            if tx.blocking_send(message).is_err() {
                break;
            }
        }

        Ok(())
    }
}

fn open_video_decoder(
    stream: &ffmpeg::format::stream::Stream<'_>,
    backend: TranscodeBackend,
) -> Result<ffmpeg::decoder::Video, String> {
    let context = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
        .map_err(|error| format!("failed to create decoder context: {}", error))?;

    match backend {
        TranscodeBackend::Software | TranscodeBackend::Pi4HardwareEncode => context
            .decoder()
            .video()
            .map_err(|error| format!("failed to open video decoder: {}", error)),
        TranscodeBackend::Pi5HardwareDecode => {
            let codec_name = match stream.parameters().id() {
                ffmpeg::codec::Id::H264 => "h264_v4l2request",
                ffmpeg::codec::Id::HEVC => "hevc_v4l2request",
                other => {
                    return Err(format!(
                        "Raspberry Pi 5 hardware decode only supports H.264/H.265 inputs; stream codec was '{:?}'",
                        other
                    ));
                }
            };

            let codec = ffmpeg::decoder::find_by_name(codec_name).ok_or_else(|| {
                format!(
                    "the local FFmpeg build does not expose the '{}' hardware decoder",
                    codec_name
                )
            })?;

            let mut decoder = context.decoder();
            decoder.set_packet_time_base(stream.time_base());
            decoder
                .open_as(codec)
                .and_then(|opened| opened.video())
                .map_err(|error| {
                    format!(
                        "failed to open Raspberry Pi 5 hardware decoder '{}': {}",
                        codec_name, error
                    )
                })
        }
    }
}

fn build_video_filter(
    decoder: &ffmpeg::decoder::Video,
    time_base: ffmpeg::Rational,
    rotation: Rotation,
) -> Result<ffmpeg::filter::Graph, String> {
    let mut filter = ffmpeg::filter::Graph::new();
    let aspect_ratio = sanitize_rational(decoder.aspect_ratio());
    let args = format!(
        "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
        decoder.width(),
        decoder.height(),
        decoder
            .format()
            .descriptor()
            .map(|descriptor| descriptor.name())
            .unwrap_or("yuv420p"),
        time_base.numerator(),
        time_base.denominator(),
        aspect_ratio.numerator(),
        aspect_ratio.denominator()
    );

    filter
        .add(
            &ffmpeg::filter::find("buffer")
                .ok_or_else(|| "FFmpeg 'buffer' filter is unavailable".to_string())?,
            "in",
            &args,
        )
        .map_err(|error| format!("failed to add buffer filter: {}", error))?;
    filter
        .add(
            &ffmpeg::filter::find("buffersink")
                .ok_or_else(|| "FFmpeg 'buffersink' filter is unavailable".to_string())?,
            "out",
            "",
        )
        .map_err(|error| format!("failed to add buffersink filter: {}", error))?;

    {
        let mut out = filter
            .get("out")
            .ok_or_else(|| "filter graph is missing the output node".to_string())?;
        out.set_pixel_format(ffmpeg::format::Pixel::YUV420P);
    }

    filter
        .output("in", 0)
        .and_then(|parser| parser.input("out", 0))
        .and_then(|parser| parser.parse(rotation.filter_spec()))
        .map_err(|error| format!("failed to parse filter graph: {}", error))?;
    filter
        .validate()
        .map_err(|error| format!("failed to validate filter graph: {}", error))?;

    Ok(filter)
}

fn open_h264_encoder(
    decoder: &ffmpeg::decoder::Video,
    processing: &CompiledProcessingProfile,
    backend: TranscodeBackend,
    output_width: u32,
    output_height: u32,
) -> Result<ffmpeg::encoder::Video, String> {
    let codec = match backend {
        TranscodeBackend::Software | TranscodeBackend::Pi5HardwareDecode => {
            ffmpeg::encoder::find_by_name("libx264")
                .or_else(|| ffmpeg::encoder::find(ffmpeg::codec::Id::H264))
                .ok_or_else(|| {
                    "no software H.264 encoder was found in the local FFmpeg build".to_string()
                })?
        }
        TranscodeBackend::Pi4HardwareEncode => {
            ffmpeg::encoder::find_by_name("h264_v4l2m2m").ok_or_else(|| {
                "the local FFmpeg build does not expose the 'h264_v4l2m2m' Raspberry Pi hardware encoder".to_string()
            })?
        }
    };

    let mut encoder = ffmpeg::codec::context::Context::new_with_codec(codec)
        .encoder()
        .video()
        .map_err(|error| format!("failed to create H.264 encoder context: {}", error))?;

    let frame_rate = processing.frame_rate as i32;
    encoder.set_width(output_width);
    encoder.set_height(output_height);
    encoder.set_aspect_ratio(decoder.aspect_ratio());
    encoder.set_format(ffmpeg::format::Pixel::YUV420P);
    encoder.set_frame_rate(Some((frame_rate, 1)));
    encoder.set_time_base((1, frame_rate));
    encoder.set_bit_rate(processing.bitrate_bps as usize);
    encoder.set_max_b_frames(0);
    encoder.set_gop(processing.frame_rate.saturating_mul(2));

    let mut options = ffmpeg::Dictionary::new();
    if !processing.preset.trim().is_empty() {
        options.set("preset", processing.preset.trim());
    }
    if !processing.tune.trim().is_empty() {
        options.set("tune", processing.tune.trim());
    }

    encoder.open_with(options).map_err(|error| match backend {
        TranscodeBackend::Software => format!("failed to open software H.264 encoder: {}", error),
        TranscodeBackend::Pi4HardwareEncode => format!(
            "failed to open Raspberry Pi 4 hardware H.264 encoder: {}",
            error
        ),
        TranscodeBackend::Pi5HardwareDecode => format!(
            "failed to open software H.264 encoder after Pi 5 hardware decode: {}",
            error
        ),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rotation {
    None,
    Clockwise,
    HalfTurn,
    CounterClockwise,
}

impl Rotation {
    fn filter_spec(self) -> &'static str {
        match self {
            Self::None => "null",
            Self::Clockwise => "transpose=clock",
            Self::HalfTurn => "hflip,vflip",
            Self::CounterClockwise => "transpose=cclock",
        }
    }
}

fn normalize_rotation(rotation: Option<i32>) -> Result<Rotation, String> {
    match rotation.unwrap_or(0) {
        0 => Ok(Rotation::None),
        90 | -270 => Ok(Rotation::Clockwise),
        180 | -180 => Ok(Rotation::HalfTurn),
        270 | -90 => Ok(Rotation::CounterClockwise),
        other => Err(format!(
            "unsupported rotation '{}'; supported values are 0, 90, 180, 270, -90, -180, and -270",
            other
        )),
    }
}

fn rotated_dimensions(width: u32, height: u32, rotation: Rotation) -> (u32, u32) {
    match rotation {
        Rotation::Clockwise | Rotation::CounterClockwise => (height, width),
        Rotation::None | Rotation::HalfTurn => (width, height),
    }
}

fn sanitize_rational(value: ffmpeg::Rational) -> ffmpeg::Rational {
    if value.numerator() <= 0 || value.denominator() <= 0 {
        ffmpeg::Rational(1, 1)
    } else {
        value
    }
}

fn open_media_input(
    input: &str,
    input_spec: &InputSpec,
) -> Result<ffmpeg::format::context::Input, String> {
    match input_spec {
        InputSpec::Rtsp { transport } => open_rtsp_input(input, *transport),
        InputSpec::Device {
            backend,
            frame_rate,
        } => open_device_input(input, *backend, *frame_rate),
    }
}

fn open_rtsp_input(
    input: &str,
    transport: Transport,
) -> Result<ffmpeg::format::context::Input, String> {
    let mut options = ffmpeg::Dictionary::new();
    options.set(
        "rtsp_transport",
        match transport {
            Transport::Tcp => "tcp",
            Transport::Udp => "udp",
        },
    );
    options.set("fflags", "nobuffer");
    options.set("flags", "low_delay");
    options.set("timeout", "5000000"); // 5 seconds for UDP
    options.set("stimeout", "5000000"); // 5 seconds for TCP/RTSP

    ffmpeg::format::input_with_dictionary(&input, options)
        .map_err(|error| format!("unable to open RTSP input '{}': {}", input, error))
}

fn open_device_input(
    input: &str,
    backend: ResolvedInputBackend,
    frame_rate: Option<u32>,
) -> Result<ffmpeg::format::context::Input, String> {
    match backend {
        ResolvedInputBackend::V4l2Device => open_v4l2_device_input(input, frame_rate),
        ResolvedInputBackend::AutoDevice => open_auto_device_input(input, frame_rate),
        ResolvedInputBackend::LibcameraDevice => Err(format!(
            "device input '{}' requested libcamera, but the current FFmpeg adapter only supports V4L2-backed capture",
            input
        )),
        ResolvedInputBackend::FfmpegRtsp => Err(format!(
            "device input '{}' resolved an RTSP backend unexpectedly",
            input
        )),
    }
}

fn open_auto_device_input(
    input: &str,
    frame_rate: Option<u32>,
) -> Result<ffmpeg::format::context::Input, String> {
    match open_v4l2_device_input(input, frame_rate) {
        Ok(context) => Ok(context),
        Err(v4l2_error) => {
            let options = device_input_options(frame_rate);
            ffmpeg::format::input_with_dictionary(&input, options).map_err(|error| {
                format!(
                    "unable to open device input '{}' via auto detection (v4l2 attempt: {}; generic attempt: {})",
                    input, v4l2_error, error
                )
            })
        }
    }
}

fn open_v4l2_device_input(
    input: &str,
    frame_rate: Option<u32>,
) -> Result<ffmpeg::format::context::Input, String> {
    let format = find_input_format("video4linux2")?;
    let options = device_input_options(frame_rate);
    ffmpeg::format::open_with(&input, &ffmpeg::Format::Input(format), options)
        .map(|context| context.input())
        .map_err(|error| format!("unable to open V4L2 device '{}': {}", input, error))
}

fn device_input_options(frame_rate: Option<u32>) -> ffmpeg::Dictionary<'static> {
    let mut options = ffmpeg::Dictionary::new();
    options.set("fflags", "nobuffer");
    options.set("flags", "low_delay");
    if let Some(frame_rate) = frame_rate.filter(|value| *value > 0) {
        options.set("framerate", &frame_rate.to_string());
    }
    options
}

fn find_input_format(name: &str) -> Result<ffmpeg::format::format::Input, String> {
    let name = CString::new(name).map_err(|_| {
        format!(
            "input format name '{}' contained an interior null byte",
            name
        )
    })?;
    let ptr = unsafe { ffmpeg::ffi::av_find_input_format(name.as_ptr()) };
    if ptr.is_null() {
        Err(format!(
            "the local FFmpeg build does not expose the '{}' input format",
            name.to_string_lossy()
        ))
    } else {
        Ok(unsafe { ffmpeg::format::format::Input::wrap(ptr.cast_mut()) })
    }
}

fn best_video_stream<'a>(
    context: &'a ffmpeg::format::context::Input,
    input: &str,
) -> Result<ffmpeg::format::stream::Stream<'a>, String> {
    context
        .streams()
        .best(ffmpeg::media::Type::Video)
        .ok_or_else(|| format!("no video stream was found in '{}'", input))
}

fn codec_name(codec: ffmpeg::codec::Id) -> Result<&'static str, String> {
    match codec {
        ffmpeg::codec::Id::H264 => Ok("h264"),
        ffmpeg::codec::Id::HEVC => Err(
            "H.265 input was detected, but the current FFmpeg backend only supports H.264 passthrough/transcode".to_string(),
        ),
        other => Err(format!(
            "unsupported encoded video codec '{:?}'; expected H.264 for the current backend",
            other
        )),
    }
}

fn frame_duration_from_processing(processing: &CompiledProcessingProfile) -> Option<Duration> {
    if processing.frame_rate == 0 {
        return None;
    }

    Some(Duration::from_nanos(
        1_000_000_000u64 / processing.frame_rate as u64,
    ))
}

fn duration_from_packet(value: i64, time_base: ffmpeg::Rational) -> Option<Duration> {
    if value <= 0 {
        return None;
    }

    duration_from_time_base(Some(value), time_base)
}

fn duration_from_time_base(value: Option<i64>, time_base: ffmpeg::Rational) -> Option<Duration> {
    let ticks = value?;
    if ticks < 0 {
        return None;
    }

    let numerator = i128::from(time_base.numerator());
    let denominator = i128::from(time_base.denominator());
    if numerator <= 0 || denominator <= 0 {
        return None;
    }

    let nanos = i128::from(ticks)
        .checked_mul(numerator)?
        .checked_mul(1_000_000_000i128)?
        .checked_div(denominator)?;
    if nanos < 0 || nanos > i128::from(u64::MAX) {
        return None;
    }

    Some(Duration::from_nanos(nanos as u64))
}

fn frame_duration_from_rate(rate: ffmpeg::Rational) -> Option<Duration> {
    let numerator = i128::from(rate.numerator());
    let denominator = i128::from(rate.denominator());
    if numerator <= 0 || denominator <= 0 {
        return None;
    }

    let nanos = 1_000_000_000i128
        .checked_mul(denominator)?
        .checked_div(numerator)?;
    if nanos <= 0 || nanos > i128::from(u64::MAX) {
        return None;
    }

    Some(Duration::from_nanos(nanos as u64))
}

fn normalize_encoded_packet(
    codec: &str,
    payload: &[u8],
    is_keyframe: bool,
    h264_config: Option<&H264Config>,
) -> Result<Vec<u8>, String> {
    match codec {
        "h264" => normalize_h264_payload(payload, is_keyframe, h264_config),
        other => Err(format!(
            "codec '{}' is not supported by the current FFmpeg source backend",
            other
        )),
    }
}

#[derive(Debug, Clone)]
struct H264Config {
    nalu_length_size: usize,
    parameter_sets_annexb: Vec<u8>,
}

fn extract_h264_config(
    parameters: &ffmpeg::codec::Parameters,
) -> Result<Option<H264Config>, String> {
    let (extradata, size) = unsafe {
        let ptr = parameters.as_ptr();
        let extradata_ptr = (*ptr).extradata;
        let extradata_size = (*ptr).extradata_size;
        if extradata_ptr.is_null() || extradata_size <= 0 {
            return Ok(None);
        }

        (
            std::slice::from_raw_parts(extradata_ptr, extradata_size as usize),
            extradata_size as usize,
        )
    };

    if size == 0 {
        return Ok(None);
    }

    if looks_like_annex_b(extradata) {
        return Ok(Some(H264Config {
            nalu_length_size: 4,
            parameter_sets_annexb: extradata.to_vec(),
        }));
    }

    if extradata[0] != 1 {
        return Ok(None);
    }

    let (nalu_length_size, parameter_sets_annexb) = parse_avcc_extradata(extradata)?;
    Ok(Some(H264Config {
        nalu_length_size,
        parameter_sets_annexb,
    }))
}

fn parse_avcc_extradata(extradata: &[u8]) -> Result<(usize, Vec<u8>), String> {
    if extradata.len() < 7 {
        return Err("AVCC extradata is too short to parse".to_string());
    }

    let nalu_length_size = ((extradata[4] & 0b11) + 1) as usize;
    let mut cursor = 5usize;
    let sps_count = (extradata[cursor] & 0b0001_1111) as usize;
    cursor += 1;

    let mut parameter_sets_annexb = Vec::new();
    for _ in 0..sps_count {
        let (next, unit) = read_avcc_unit(extradata, cursor)?;
        cursor = next;
        parameter_sets_annexb.extend_from_slice(H264_START_CODE);
        parameter_sets_annexb.extend_from_slice(unit);
    }

    if cursor >= extradata.len() {
        return Ok((nalu_length_size, parameter_sets_annexb));
    }

    let pps_count = extradata[cursor] as usize;
    cursor += 1;
    for _ in 0..pps_count {
        let (next, unit) = read_avcc_unit(extradata, cursor)?;
        cursor = next;
        parameter_sets_annexb.extend_from_slice(H264_START_CODE);
        parameter_sets_annexb.extend_from_slice(unit);
    }

    Ok((nalu_length_size, parameter_sets_annexb))
}

fn read_avcc_unit(data: &[u8], cursor: usize) -> Result<(usize, &[u8]), String> {
    if cursor + 2 > data.len() {
        return Err("AVCC extradata ended before a NAL length field".to_string());
    }

    let size = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
    let start = cursor + 2;
    let end = start
        .checked_add(size)
        .ok_or_else(|| "AVCC NAL length overflowed".to_string())?;
    if end > data.len() {
        return Err("AVCC extradata contained a truncated NAL unit".to_string());
    }

    Ok((end, &data[start..end]))
}

fn normalize_h264_payload(
    payload: &[u8],
    is_keyframe: bool,
    config: Option<&H264Config>,
) -> Result<Vec<u8>, String> {
    let mut output = if looks_like_annex_b(payload) {
        payload.to_vec()
    } else {
        avcc_to_annex_b(
            payload,
            config.map(|value| value.nalu_length_size).unwrap_or(4),
        )?
    };

    if is_keyframe {
        if let Some(config) = config {
            let (has_sps, has_pps) = annex_b_parameter_sets(&output);
            if !has_sps || !has_pps {
                let mut prefixed =
                    Vec::with_capacity(config.parameter_sets_annexb.len() + output.len());
                prefixed.extend_from_slice(&config.parameter_sets_annexb);
                prefixed.extend_from_slice(&output);
                output = prefixed;
            }
        }
    }

    Ok(output)
}

fn avcc_to_annex_b(payload: &[u8], nalu_length_size: usize) -> Result<Vec<u8>, String> {
    if !(1..=4).contains(&nalu_length_size) {
        return Err(format!(
            "invalid AVCC NALU length size {}; expected 1-4 bytes",
            nalu_length_size
        ));
    }

    let mut cursor = 0usize;
    let mut output = Vec::with_capacity(payload.len() + 32);

    while cursor < payload.len() {
        if cursor + nalu_length_size > payload.len() {
            return Err("AVCC payload ended before a NALU size field".to_string());
        }

        let mut unit_length = 0usize;
        for byte in &payload[cursor..cursor + nalu_length_size] {
            unit_length = (unit_length << 8) | usize::from(*byte);
        }
        cursor += nalu_length_size;

        if unit_length == 0 {
            continue;
        }

        let end = cursor
            .checked_add(unit_length)
            .ok_or_else(|| "AVCC NALU length overflowed".to_string())?;
        if end > payload.len() {
            return Err("AVCC payload contained a truncated NALU".to_string());
        }

        output.extend_from_slice(H264_START_CODE);
        output.extend_from_slice(&payload[cursor..end]);
        cursor = end;
    }

    if output.is_empty() && !payload.is_empty() {
        return Err("AVCC payload produced no NAL units".to_string());
    }

    Ok(output)
}

fn looks_like_annex_b(payload: &[u8]) -> bool {
    payload.starts_with(H264_START_CODE) || payload.starts_with(&[0x00, 0x00, 0x01])
}

fn annex_b_parameter_sets(payload: &[u8]) -> (bool, bool) {
    let mut has_sps = false;
    let mut has_pps = false;
    let mut cursor = 0usize;

    while let Some((start, code_len)) = find_start_code(payload, cursor) {
        let nalu_start = start + code_len;
        let next = find_start_code(payload, nalu_start)
            .map(|(offset, _)| offset)
            .unwrap_or(payload.len());
        if nalu_start < next {
            let nalu_type = payload[nalu_start] & 0x1F;
            if nalu_type == 7 {
                has_sps = true;
            } else if nalu_type == 8 {
                has_pps = true;
            }
        }
        cursor = next;
    }

    (has_sps, has_pps)
}

fn find_start_code(payload: &[u8], offset: usize) -> Option<(usize, usize)> {
    let mut index = offset;
    while index + 3 <= payload.len() {
        if index + 4 <= payload.len() && payload[index..index + 4] == *H264_START_CODE {
            return Some((index, 4));
        }
        if payload[index..index + 3] == [0x00, 0x00, 0x01] {
            return Some((index, 3));
        }
        index += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use caml_core::{
        CodecPath, CompiledInput, CompiledPipeline, CompiledProcessingProfile, ExecutionMode,
        RecoveryPolicy, ResolvedInputBackend, RuntimeError, RuntimePolicy, SourceFactory,
        StreamStrategy,
    };

    use super::{
        frame_duration_from_processing, normalize_h264_payload, normalize_rotation,
        parse_avcc_extradata, rotated_dimensions, transcode_backend_for_pipeline,
        worker_spec_for_pipeline, FfmpegSourceFactory, H264Config, InputSpec, Rotation,
        TranscodeBackend, WorkerMode, H264_START_CODE,
    };

    #[test]
    fn parses_avcc_extradata_and_converts_payload_to_annex_b() {
        let extradata = vec![
            0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1F, 0x01, 0x00,
            0x04, 0x68, 0xEE, 0x3C, 0x80,
        ];
        let (nalu_length_size, parameter_sets_annexb) =
            parse_avcc_extradata(&extradata).expect("extradata should parse");
        assert_eq!(nalu_length_size, 4);
        assert_eq!(
            parameter_sets_annexb,
            vec![
                0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x01, 0x68, 0xEE,
                0x3C, 0x80
            ]
        );

        let payload = vec![0x00, 0x00, 0x00, 0x03, 0x65, 0x88, 0x84];
        let normalized = normalize_h264_payload(
            &payload,
            true,
            Some(&H264Config {
                nalu_length_size,
                parameter_sets_annexb,
            }),
        )
        .expect("payload should normalize");

        assert!(normalized.starts_with(H264_START_CODE));
        assert_eq!(
            &normalized[16..],
            &[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84]
        );
    }

    #[test]
    fn leaves_existing_annex_b_keyframe_intact_when_parameter_sets_are_present() {
        let payload = vec![
            0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x01, 0x68, 0xEE,
            0x3C, 0x80, 0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84,
        ];
        let normalized = normalize_h264_payload(&payload, true, None).expect("payload should pass");
        assert_eq!(normalized, payload);
    }

    #[test]
    fn normalizes_supported_rotation_values() {
        assert_eq!(normalize_rotation(Some(90)).unwrap(), Rotation::Clockwise);
        assert_eq!(
            normalize_rotation(Some(-90)).unwrap(),
            Rotation::CounterClockwise
        );
        assert_eq!(normalize_rotation(Some(180)).unwrap(), Rotation::HalfTurn);
        assert_eq!(normalize_rotation(None).unwrap(), Rotation::None);
        assert!(normalize_rotation(Some(45)).is_err());
    }

    #[test]
    fn swaps_dimensions_for_quarter_turns() {
        assert_eq!(rotated_dimensions(1920, 1080, Rotation::None), (1920, 1080));
        assert_eq!(
            rotated_dimensions(1920, 1080, Rotation::Clockwise),
            (1080, 1920)
        );
    }

    #[test]
    fn derives_transcode_frame_duration() {
        let processing = CompiledProcessingProfile {
            codec: "h264".to_string(),
            encoder: "software".to_string(),
            preset: "ultrafast".to_string(),
            tune: "zerolatency".to_string(),
            frame_rate: 25,
            bitrate_bps: 512_000,
            rotation: None,
        };
        assert_eq!(
            frame_duration_from_processing(&processing),
            Some(Duration::from_millis(40))
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_transcode_codec_in_factory() {
        let factory = FfmpegSourceFactory;
        let pipeline = compiled_pipeline("h265", "software", Some(90));
        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("unsupported codec should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error
            .to_string()
            .contains("only implements software H.264 transcode"));
    }

    #[tokio::test]
    async fn rejects_unsupported_rotation_in_factory() {
        let factory = FfmpegSourceFactory;
        let pipeline = compiled_pipeline("h264", "software", Some(45));
        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("unsupported rotation should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error.to_string().contains("unsupported rotation"));
    }

    #[test]
    fn plans_v4l2_device_capture_for_device_inputs() {
        let mut pipeline = compiled_pipeline("h264", "software", Some(90));
        pipeline.input.kind = InputType::Device;
        pipeline.input.source = "/dev/video0".to_string();
        pipeline.resolved_backend = ResolvedInputBackend::V4l2Device;

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        assert_eq!(
            spec.input_spec,
            InputSpec::Device {
                backend: ResolvedInputBackend::V4l2Device,
                frame_rate: Some(25),
            }
        );
    }

    #[test]
    fn selects_pi4_hardware_transcode_backend() {
        let mut pipeline = compiled_pipeline("h264", "v4l2m2m", None);
        pipeline.codec_path = CodecPath::HardwareTranscode;

        assert_eq!(
            transcode_backend_for_pipeline(&pipeline).expect("backend should resolve"),
            TranscodeBackend::Pi4HardwareEncode
        );

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        match spec.mode {
            WorkerMode::Transcode { backend, .. } => {
                assert_eq!(backend, TranscodeBackend::Pi4HardwareEncode);
            }
            other => panic!("expected transcode worker mode, got {:?}", other),
        }
    }

    #[test]
    fn selects_pi5_hardware_decode_backend() {
        let mut pipeline = compiled_pipeline("h264", "software", None);
        pipeline.strategy = StreamStrategy::HardwareDecode;
        pipeline.codec_path = CodecPath::HardwareDecode;

        assert_eq!(
            transcode_backend_for_pipeline(&pipeline).expect("backend should resolve"),
            TranscodeBackend::Pi5HardwareDecode
        );

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        match spec.mode {
            WorkerMode::Transcode { backend, .. } => {
                assert_eq!(backend, TranscodeBackend::Pi5HardwareDecode);
            }
            other => panic!("expected transcode worker mode, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rejects_libcamera_device_backend_in_ffmpeg_factory() {
        let factory = FfmpegSourceFactory;
        let mut pipeline = compiled_pipeline("h264", "software", None);
        pipeline.input.kind = InputType::Device;
        pipeline.input.source = "/dev/video0".to_string();
        pipeline.resolved_backend = ResolvedInputBackend::LibcameraDevice;

        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("libcamera backend should fail in ffmpeg adapter"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error.to_string().contains("libcamera"));
    }

    fn compiled_pipeline(codec: &str, encoder: &str, rotation: Option<i32>) -> CompiledPipeline {
        CompiledPipeline {
            id: "camera_a".to_string(),
            input: CompiledInput {
                kind: InputType::Rtsp,
                source: "rtsp://127.0.0.1:8554/live".to_string(),
            },
            strategy: StreamStrategy::Transcode,
            outputs: vec![],
            network: Some(caml_core::CompiledNetworkProfile {
                transport: Transport::Tcp,
                packet_size_limit: 1200,
                stall_timeout: Duration::from_secs(1),
            }),
            processing: Some(CompiledProcessingProfile {
                codec: codec.to_string(),
                encoder: encoder.to_string(),
                preset: "ultrafast".to_string(),
                tune: "zerolatency".to_string(),
                frame_rate: 25,
                bitrate_bps: 512_000,
                rotation,
            }),
            runtime: RuntimePolicy {
                buffer_size: 1200,
                watchdog_timeout: Duration::from_secs(1),
            },
            resolved_backend: ResolvedInputBackend::FfmpegRtsp,
            execution_mode: ExecutionMode::DecodedFrames,
            codec_path: CodecPath::SoftwareTranscode,
            recovery: RecoveryPolicy {
                class: caml_core::RecoveryClass::Device,
                max_restarts: 0,
                restart_backoff: Duration::from_millis(1),
            },
            capability_requirements: Vec::new(),
        }
    }

    use std::time::Duration;

    use caml_core::{InputType, Transport};
}
