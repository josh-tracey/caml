use std::ffi::CString;
use std::time::Duration;
use ffmpeg_next as ffmpeg;

use caml_core::{CompiledProcessingProfile, ResolvedInputBackend};

use crate::h264::{H264Config, normalize_h264_payload};

pub fn open_media_input(
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

pub fn open_rtsp_input(
    input: &str,
    transport: caml_core::Transport,
) -> Result<ffmpeg::format::context::Input, String> {
    let mut options = ffmpeg::Dictionary::new();
    options.set(
        "rtsp_transport",
        match transport {
            caml_core::Transport::Tcp => "tcp",
            caml_core::Transport::Udp => "udp",
        },
    );
    options.set("fflags", "nobuffer");
    options.set("flags", "low_delay");
    options.set("timeout", "5000000"); // 5 seconds for UDP
    options.set("stimeout", "5000000"); // 5 seconds for TCP/RTSP

    ffmpeg::format::input_with_dictionary(&input, options)
        .map_err(|error| format!("unable to open RTSP input '{}': {}", input, error))
}

pub fn open_device_input(
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

pub fn open_auto_device_input(
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

pub fn open_v4l2_device_input(
    input: &str,
    frame_rate: Option<u32>,
) -> Result<ffmpeg::format::context::Input, String> {
    let format = find_input_format("video4linux2")?;
    let options = device_input_options(frame_rate);
    ffmpeg::format::open_with(&input, &ffmpeg::Format::Input(format), options)
        .map(|context| context.input())
        .map_err(|error| format!("unable to open V4L2 device '{}': {}", input, error))
}

pub fn device_input_options(frame_rate: Option<u32>) -> ffmpeg::Dictionary<'static> {
    let mut options = ffmpeg::Dictionary::new();
    options.set("fflags", "nobuffer");
    options.set("flags", "low_delay");
    if let Some(frame_rate) = frame_rate.filter(|value| *value > 0) {
        options.set("framerate", &frame_rate.to_string());
    }
    options
}

pub fn find_input_format(name: &str) -> Result<ffmpeg::format::format::Input, String> {
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

pub fn best_video_stream<'a>(
    context: &'a ffmpeg::format::context::Input,
    input: &str,
) -> Result<ffmpeg::format::stream::Stream<'a>, String> {
    context
        .streams()
        .best(ffmpeg::media::Type::Video)
        .ok_or_else(|| format!("no video stream was found in '{}'", input))
}

pub fn codec_name(codec: ffmpeg::codec::Id) -> Result<&'static str, String> {
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

pub fn frame_duration_from_processing(processing: &CompiledProcessingProfile) -> Option<Duration> {
    if processing.frame_rate == 0 {
        return None;
    }

    Some(Duration::from_nanos(
        1_000_000_000u64 / processing.frame_rate as u64,
    ))
}

pub fn duration_from_packet(value: i64, time_base: ffmpeg::Rational) -> Option<Duration> {
    if value <= 0 {
        return None;
    }

    duration_from_time_base(Some(value), time_base)
}

pub fn duration_from_time_base(value: Option<i64>, time_base: ffmpeg::Rational) -> Option<Duration> {
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

pub fn frame_duration_from_rate(rate: ffmpeg::Rational) -> Option<Duration> {
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

pub fn normalize_encoded_packet(
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

// We import InputSpec from worker, but since we have a circular dependency or reference,
// let's define InputSpec or use the module reference.
// Let's use the Worker types. We will import InputSpec from crate::worker.
use crate::worker::InputSpec;
