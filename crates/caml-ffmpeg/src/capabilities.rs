use std::sync::OnceLock;
use caml_core::{HostCapabilities, StaticCapabilityProbe};
use ffmpeg_next as ffmpeg;

pub fn init_ffmpeg() -> bool {
    static INIT_RESULT: OnceLock<bool> = OnceLock::new();
    *INIT_RESULT.get_or_init(|| ffmpeg::init().is_ok())
}

pub fn ffmpeg_capabilities() -> StaticCapabilityProbe {
    if !init_ffmpeg() {
        return StaticCapabilityProbe::new(HostCapabilities::default());
    }

    let mut caps = HostCapabilities {
        ffmpeg_available: true,
        v4l2_available: false,
        libcamera_available: false,
        rtp_packetization_available: false,
        pi_model: None,
        has_pi4_h264_encoder: false,
        has_pi5_stateless_decoder: false,
    };

    // Probe deep ffmpeg capabilities
    if ffmpeg::encoder::find_by_name("h264_v4l2m2m").is_some() {
        caps.has_pi4_h264_encoder = true;
    }

    if ffmpeg::decoder::find_by_name("h264_v4l2request").is_some()
        || ffmpeg::decoder::find_by_name("hevc_v4l2request").is_some()
    {
        caps.has_pi5_stateless_decoder = true;
    }

    let v4l2_name = std::ffi::CString::new("video4linux2").unwrap();
    if unsafe { !ffmpeg::ffi::av_find_input_format(v4l2_name.as_ptr()).is_null() } {
        caps.v4l2_available = true;
    }

    StaticCapabilityProbe::new(caps)
}
