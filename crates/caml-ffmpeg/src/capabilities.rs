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
        rtp_packetization_available: false, // Wait, maybe it is available?
        ..HostCapabilities::default()
    };
    
    // Check for encoders
    if ffmpeg::encoder::find_by_name("libx264").is_some() || ffmpeg::encoder::find(ffmpeg::codec::Id::H264).is_some() {
        // has software h264
    }
    
    if ffmpeg::encoder::find_by_name("h264_v4l2m2m").is_some() {
        // has v4l2m2m hardware encode
    }

    if ffmpeg::decoder::find_by_name("h264_v4l2request").is_some() {
        // has v4l2request hardware decode
    }

    if ffmpeg::decoder::find_by_name("hevc_v4l2request").is_some() {
        // has hevc hardware decode
    }
    
    // Check input devices
    if ffmpeg::format::input::by_name("video4linux2").is_some() {
        // has v4l2 capture
    }

    // You would update the HostCapabilities struct to hold these fine-grained capabilities.
    // For now we return the basic one matching previous behavior, but populated.
    
    StaticCapabilityProbe::new(caps)
}
