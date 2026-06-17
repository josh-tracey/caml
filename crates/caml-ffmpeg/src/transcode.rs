use std::{path::Path, time::Duration};
use tokio::sync::mpsc;
use ffmpeg_next as ffmpeg;

use caml_core::{
    CompiledOverlayLayer, CompiledOverlayProfile, CompiledPipeline, CompiledProcessingProfile,
    CompiledTextOverlayStyle, CompiledTimestampOverlay, CompiledWatermarkOverlay,
    OverlayPosition, OverlayTimezone,
};

use crate::hwaccel::TranscodeBackend;
use crate::worker::{WorkerMessage, OwnedEncodedPacket, InputSpec};
use crate::device::{
    open_media_input, best_video_stream, frame_duration_from_processing, duration_from_packet,
    duration_from_time_base,
};
use crate::h264::{H264Config, extract_h264_config, normalize_h264_payload};

use tokio_util::sync::CancellationToken;

pub fn transcode_packets(
    job: TranscodeJob<'_>,
) -> Result<(), String> {
    let mut context = open_media_input(job.input, job.input_spec)?;
    let nominal_duration =
        frame_duration_from_processing(job.processing).unwrap_or(job.default_frame_duration);
    let (stream_index, mut transcoder) = {
        let stream = best_video_stream(&context, job.input)?;
        (
            stream.index(),
            VideoTranscoder::new(
                &stream,
                job.processing,
                job.overlay,
                job.backend,
                nominal_duration,
            )?,
        )
    };

    let start_instant = std::time::Instant::now();
    let mut first_pts_duration = None;

    for (stream, packet) in context.packets() {
        if job.cancel.is_cancelled() {
            break;
        }

        if stream.index() != stream_index {
            continue;
        }

        if job.is_local_file {
            if let Some(pts) = packet.pts() {
                let pts_duration = duration_from_time_base(Some(pts), stream.time_base()).unwrap_or(Duration::ZERO);
                let first_pts = *first_pts_duration.get_or_insert(pts_duration);
                let relative_duration = pts_duration.checked_sub(first_pts).unwrap_or(Duration::ZERO);
                let target_time = start_instant + relative_duration;
                let now = std::time::Instant::now();
                if target_time > now {
                    std::thread::sleep(target_time - now);
                }
            }
        }

        transcoder.send_packet_to_decoder(&packet)?;
        transcoder.receive_decoded_frames(job.tx)?;
    }

    transcoder.send_eof_to_decoder()?;
    transcoder.receive_decoded_frames(job.tx)?;
    transcoder.flush_filter()?;
    transcoder.receive_filtered_frames(job.tx)?;
    transcoder.send_eof_to_encoder()?;
    transcoder.receive_encoded_packets(job.tx)?;

    Ok(())
}

pub struct TranscodeJob<'a> {
    pub input: &'a str,
    pub input_spec: &'a InputSpec,
    pub processing: &'a CompiledProcessingProfile,
    pub overlay: Option<&'a CompiledOverlayProfile>,
    pub backend: TranscodeBackend,
    pub default_frame_duration: Duration,
    pub tx: &'a mpsc::Sender<WorkerMessage>,
    pub cancel: &'a CancellationToken,
    pub is_local_file: bool,
}

pub struct VideoTranscoder {
    decoder: ffmpeg::decoder::Video,
    filter: ffmpeg::filter::Graph,
    encoder: ffmpeg::encoder::Video,
    encoder_time_base: ffmpeg::Rational,
    encoder_h264_config: Option<H264Config>,
    nominal_duration: Duration,
}

impl VideoTranscoder {
    pub fn new(
        stream: &ffmpeg::format::stream::Stream<'_>,
        processing: &CompiledProcessingProfile,
        overlay: Option<&CompiledOverlayProfile>,
        backend: TranscodeBackend,
        nominal_duration: Duration,
    ) -> Result<Self, String> {
        let decoder = open_video_decoder(stream, backend)?;

        let rotation = normalize_rotation(processing.rotation)?;
        let (output_width, output_height) =
            rotated_dimensions(decoder.width(), decoder.height(), rotation);
        let filter = build_video_filter(&decoder, stream.time_base(), rotation, overlay)?;
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

    pub fn send_packet_to_decoder(&mut self, packet: &ffmpeg::Packet) -> Result<(), String> {
        self.decoder
            .send_packet(packet)
            .map_err(|error| format!("failed to send packet to decoder: {}", error))
    }

    pub fn send_eof_to_decoder(&mut self) -> Result<(), String> {
        self.decoder
            .send_eof()
            .map_err(|error| format!("failed to send decoder EOF: {}", error))
    }

    pub fn send_eof_to_encoder(&mut self) -> Result<(), String> {
        self.encoder
            .send_eof()
            .map_err(|error| format!("failed to send encoder EOF: {}", error))
    }

    pub fn receive_decoded_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
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

    pub fn flush_filter(&mut self) -> Result<(), String> {
        let mut source = self
            .filter
            .get("in")
            .ok_or_else(|| "filter graph is missing the input node".to_string())?;
        source
            .source()
            .flush()
            .map_err(|error| format!("failed to flush video filter graph: {}", error))
    }

    pub fn receive_filtered_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
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

    pub fn receive_encoded_packets(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
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

pub fn open_video_decoder(
    stream: &ffmpeg::format::stream::Stream<'_>,
    backend: TranscodeBackend,
) -> Result<ffmpeg::decoder::Video, String> {
    let mut context = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
        .map_err(|error| format!("failed to create decoder context: {}", error))?;

    // Optimize decoding latency by disabling frame-parallel buffering (auto-threading buffers frames)
    let mut decoder_threading = ffmpeg::threading::Config::kind(ffmpeg::threading::Type::Slice);
    decoder_threading.count = 1;
    context.set_threading(decoder_threading);

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

pub fn build_video_filter(
    decoder: &ffmpeg::decoder::Video,
    time_base: ffmpeg::Rational,
    rotation: Rotation,
    overlay: Option<&CompiledOverlayProfile>,
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

    let filter_spec = build_filter_spec(rotation, overlay)?;

    filter
        .output("in", 0)
        .and_then(|parser| parser.input("out", 0))
        .and_then(|parser| parser.parse(&filter_spec))
        .map_err(|error| format!("failed to parse filter graph: {}", error))?;
    filter
        .validate()
        .map_err(|error| format!("failed to validate filter graph: {}", error))?;

    Ok(filter)
}

pub const DEFAULT_DRAW_TEXT_FONT_PATH: &str =
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

pub fn validate_overlay_runtime(pipeline: &CompiledPipeline) -> Result<(), String> {
    let Some(overlay) = pipeline.overlay.as_ref() else {
        return Ok(());
    };

    if !crate::capabilities::init_ffmpeg() {
        return Err(format!(
            "pipeline '{}' requires FFmpeg overlay support, but FFmpeg failed to initialize",
            pipeline.id
        ));
    }

    let uses_text = overlay.layers.iter().any(|layer| {
        matches!(
            layer,
            CompiledOverlayLayer::Timestamp(_) | CompiledOverlayLayer::Text(_)
        )
    });

    if uses_text && !Path::new(DEFAULT_DRAW_TEXT_FONT_PATH).exists() {
        return Err(format!(
            "pipeline '{}' requires the default overlay font '{}', but it was not found; install a DejaVu Sans font package on the host",
            pipeline.id, DEFAULT_DRAW_TEXT_FONT_PATH
        ));
    }

    for layer in &overlay.layers {
        if let CompiledOverlayLayer::Watermark(watermark) = layer {
            validate_watermark_asset(&pipeline.id, watermark)?;
        }
    }

    Ok(())
}

fn validate_watermark_asset(
    pipeline_id: &str,
    watermark: &CompiledWatermarkOverlay,
) -> Result<(), String> {
    let path = Path::new(&watermark.image_path);
    let metadata = std::fs::metadata(path).map_err(|error| {
        format!(
            "pipeline '{}' watermark '{}' is not accessible: {}",
            pipeline_id, watermark.image_path, error
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "pipeline '{}' watermark '{}' must be a regular file",
            pipeline_id, watermark.image_path
        ));
    }

    let context = ffmpeg::format::input(&watermark.image_path).map_err(|error| {
        format!(
            "pipeline '{}' watermark '{}' could not be decoded by FFmpeg: {}",
            pipeline_id, watermark.image_path, error
        )
    })?;
    best_video_stream(&context, &watermark.image_path).map(|_| ())
}

fn build_filter_spec(
    rotation: Rotation,
    overlay: Option<&CompiledOverlayProfile>,
) -> Result<String, String> {
    let Some(overlay) = overlay else {
        return Ok(format!("[in]{}[out]", rotation.filter_spec()));
    };

    let mut segments = Vec::new();
    let mut current = "stage0".to_string();
    segments.push(format!("[in]{}[{}]", rotation.filter_spec(), current));

    for (index, layer) in overlay.layers.iter().enumerate() {
        let next = format!("stage{}", index + 1);
        match layer {
            CompiledOverlayLayer::Timestamp(timestamp) => {
                segments.push(format!(
                    "[{}]{}[{}]",
                    current,
                    drawtext_filter_for_timestamp(timestamp)?,
                    next
                ));
            }
            CompiledOverlayLayer::Text(text) => {
                segments.push(format!(
                    "[{}]{}[{}]",
                    current,
                    drawtext_filter_for_text(&text.text, &text.style)?,
                    next
                ));
            }
            CompiledOverlayLayer::Watermark(watermark) => {
                let wm_label = format!("wm{}", index);
                segments.push(format!("{}[{}]", watermark_filter_chain(watermark)?, wm_label));
                segments.push(format!(
                    "[{}][{}]overlay={}[{}]",
                    current,
                    wm_label,
                    overlay_position_expr(watermark.position, watermark.margin),
                    next
                ));
            }
        }
        current = next;
    }

    segments.push(format!("[{}]null[out]", current));
    Ok(segments.join(";"))
}

fn drawtext_filter_for_timestamp(timestamp: &CompiledTimestampOverlay) -> Result<String, String> {
    let style = &timestamp.style;
    let time_fn = match timestamp.timezone {
        OverlayTimezone::Utc => "gmtime",
        OverlayTimezone::Local => "localtime",
    };
    let format = escape_drawtext_expansion_format(&timestamp.format);
    let text = format!("%{{{}\\:{}}}", time_fn, format);
    drawtext_filter(&text, style)
}

fn drawtext_filter_for_text(
    text: &str,
    style: &CompiledTextOverlayStyle,
) -> Result<String, String> {
    let text = escape_drawtext_text(text);
    drawtext_filter(&text, style)
}

fn drawtext_filter(
    text: &str,
    style: &CompiledTextOverlayStyle,
) -> Result<String, String> {
    let font_path = escape_filter_path(DEFAULT_DRAW_TEXT_FONT_PATH);
    let font_color = escape_filter_value(&style.font_color);
    let box_color = format!(
        "{}@{}",
        escape_filter_value(&style.background_color),
        alpha_percent_to_decimal(style.background_alpha)
    );
    let (x, y) = drawtext_position_expr(style.position, style.margin);

    Ok(format!(
        "drawtext=fontfile='{}':text='{}':x={}:y={}:fontsize={}:fontcolor={}:box=1:boxcolor={}:boxborderw={}",
        font_path, text, x, y, style.font_size, font_color, box_color, style.padding
    ))
}

fn watermark_filter_chain(watermark: &CompiledWatermarkOverlay) -> Result<String, String> {
    let path = escape_filter_path(&watermark.image_path);
    let mut chain = format!("movie='{}'", path);

    if let Some(max_width_px) = watermark.max_width_px {
        chain.push_str(&format!(
            ",scale=w={}:h=-1:force_original_aspect_ratio=decrease",
            max_width_px
        ));
    }

    if watermark.opacity < 100 {
        chain.push_str(",format=rgba");
        chain.push_str(&format!(
            ",colorchannelmixer=aa={}",
            alpha_percent_to_decimal(watermark.opacity)
        ));
    }

    Ok(chain)
}

fn drawtext_position_expr(position: OverlayPosition, margin: u32) -> (String, String) {
    match position {
        OverlayPosition::TopLeft => (format!("{margin}"), format!("{margin}")),
        OverlayPosition::TopRight => (
            format!("w-text_w-{margin}"),
            format!("{margin}"),
        ),
        OverlayPosition::BottomLeft => (
            format!("{margin}"),
            format!("h-text_h-{margin}"),
        ),
        OverlayPosition::BottomRight => (
            format!("w-text_w-{margin}"),
            format!("h-text_h-{margin}"),
        ),
    }
}

fn overlay_position_expr(position: OverlayPosition, margin: u32) -> String {
    match position {
        OverlayPosition::TopLeft => format!("x={}:y={}", margin, margin),
        OverlayPosition::TopRight => format!("x=W-w-{}:y={}", margin, margin),
        OverlayPosition::BottomLeft => format!("x={}:y=H-h-{}", margin, margin),
        OverlayPosition::BottomRight => format!("x=W-w-{}:y=H-h-{}", margin, margin),
    }
}

fn escape_filter_path(value: &str) -> String {
    escape_filter_value(value)
}

fn escape_filter_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(':', "\\:")
        .replace('\'', "\\'")
}

fn escape_drawtext_text(value: &str) -> String {
    escape_filter_value(value).replace('%', "\\%")
}

fn escape_drawtext_expansion_format(value: &str) -> String {
    value
        .replace('\\', "\\\\\\\\")
        .replace(':', "\\\\\\:")
        .replace('\'', "\\'")
}

fn alpha_percent_to_decimal(value: u8) -> String {
    let alpha = f32::from(value) / 100.0;
    format!("{alpha:.2}")
}

pub fn open_h264_encoder(
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

    // Optimize encoding latency by disabling frame-parallel buffering
    let mut encoder_threading = ffmpeg::threading::Config::kind(ffmpeg::threading::Type::Slice);
    encoder_threading.count = 1;
    encoder.set_threading(encoder_threading);

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
pub enum Rotation {
    None,
    Clockwise,
    HalfTurn,
    CounterClockwise,
}

impl Rotation {
    pub fn filter_spec(self) -> &'static str {
        match self {
            Self::None => "null",
            Self::Clockwise => "transpose=clock",
            Self::HalfTurn => "hflip,vflip",
            Self::CounterClockwise => "transpose=cclock",
        }
    }
}

pub fn normalize_rotation(rotation: Option<i32>) -> Result<Rotation, String> {
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

pub fn rotated_dimensions(width: u32, height: u32, rotation: Rotation) -> (u32, u32) {
    match rotation {
        Rotation::Clockwise | Rotation::CounterClockwise => (height, width),
        Rotation::None | Rotation::HalfTurn => (width, height),
    }
}

pub fn sanitize_rational(value: ffmpeg::Rational) -> ffmpeg::Rational {
    if value.numerator() <= 0 || value.denominator() <= 0 {
        ffmpeg::Rational(1, 1)
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::{
        alpha_percent_to_decimal, build_filter_spec, drawtext_filter_for_timestamp,
        validate_overlay_runtime, Rotation,
    };
    use caml_core::{
        CodecPath, CompiledInput, CompiledOverlayLayer, CompiledOverlayProfile, CompiledPipeline,
        CompiledTextOverlay, CompiledTextOverlayStyle, CompiledTimestampOverlay,
        CompiledWatermarkOverlay, ExecutionMode, InputType, OverlayPosition, OverlayTimezone,
        RecoveryClass, RecoveryPolicy, ResolvedInputBackend, RuntimePolicy, StreamStrategy,
    };
    use std::time::Duration;

    fn overlay_pipeline(overlay: CompiledOverlayProfile) -> CompiledPipeline {
        CompiledPipeline {
            id: "BOTTOM_CAM01".to_string(),
            input: CompiledInput {
                kind: InputType::Device,
                source: "/dev/video0".to_string(),
            },
            strategy: StreamStrategy::Transcode,
            network: None,
            processing: None,
            overlay: Some(overlay),
            runtime: RuntimePolicy {
                buffer_size: 1200,
                watchdog_timeout: Duration::from_secs(5),
                buffer_count: 64,
            },
            resolved_backend: ResolvedInputBackend::AutoDevice,
            execution_mode: ExecutionMode::DecodedFrames,
            codec_path: CodecPath::SoftwareTranscode,
            recovery: RecoveryPolicy {
                class: RecoveryClass::Device,
                max_restarts: 3,
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(30),
                backoff_multiplier: 2.0,
                reset_after: Duration::from_secs(60),
            },
            capability_requirements: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[test]
    fn builds_filter_spec_in_overlay_order() {
        let overlay = CompiledOverlayProfile {
            layers: vec![
                CompiledOverlayLayer::Timestamp(CompiledTimestampOverlay {
                    format: "%Y-%m-%d %H:%M:%S UTC".to_string(),
                    timezone: OverlayTimezone::Utc,
                    style: CompiledTextOverlayStyle {
                        position: OverlayPosition::TopLeft,
                        font_size: 18,
                        font_color: "white".to_string(),
                        background_color: "black".to_string(),
                        background_alpha: 60,
                        padding: 6,
                        margin: 12,
                    },
                }),
                CompiledOverlayLayer::Text(CompiledTextOverlay {
                    text: "DEV01".to_string(),
                    style: CompiledTextOverlayStyle {
                        position: OverlayPosition::BottomRight,
                        font_size: 18,
                        font_color: "yellow".to_string(),
                        background_color: "black".to_string(),
                        background_alpha: 50,
                        padding: 4,
                        margin: 10,
                    },
                }),
                CompiledOverlayLayer::Watermark(CompiledWatermarkOverlay {
                    image_path: "./logo.png".to_string(),
                    position: OverlayPosition::BottomRight,
                    max_width_px: Some(96),
                    opacity: 75,
                    margin: 8,
                }),
            ],
        };

        let spec = build_filter_spec(Rotation::Clockwise, Some(&overlay)).expect("spec");
        assert!(spec.contains("[in]transpose=clock[stage0]"));
        assert!(spec.contains("drawtext"));
        assert!(spec.contains("movie='./logo.png'"));

        let first_draw = spec.find("drawtext").expect("first drawtext");
        let second_draw = spec[first_draw + 1..]
            .find("drawtext")
            .map(|offset| offset + first_draw + 1)
            .expect("second drawtext");
        let movie = spec.find("movie='./logo.png'").expect("movie");
        assert!(first_draw < second_draw);
        assert!(second_draw < movie);
    }

    #[test]
    fn rejects_missing_watermark_asset_before_worker_start() {
        let overlay = CompiledOverlayProfile {
            layers: vec![CompiledOverlayLayer::Watermark(CompiledWatermarkOverlay {
                image_path: "./does-not-exist.png".to_string(),
                position: OverlayPosition::TopLeft,
                max_width_px: None,
                opacity: 100,
                margin: 12,
            })],
        };

        let error = validate_overlay_runtime(&overlay_pipeline(overlay))
            .expect_err("missing watermark should fail");
        assert!(error.contains("does-not-exist.png"));
    }

    #[test]
    fn converts_alpha_percent_to_decimal() {
        assert_eq!(alpha_percent_to_decimal(75), "0.75");
    }

    #[test]
    fn timestamp_overlay_escapes_inner_time_format_colons() {
        let filter = drawtext_filter_for_timestamp(&CompiledTimestampOverlay {
            format: "%Y-%m-%d %H:%M:%S UTC".to_string(),
            timezone: OverlayTimezone::Utc,
            style: CompiledTextOverlayStyle {
                position: OverlayPosition::TopLeft,
                font_size: 18,
                font_color: "white".to_string(),
                background_color: "black".to_string(),
                background_alpha: 60,
                padding: 6,
                margin: 12,
            },
        })
        .expect("timestamp filter");

        assert!(filter.contains("%{gmtime\\:%Y-%m-%d %H\\\\\\:%M\\\\\\:%S UTC}"));
    }
}
