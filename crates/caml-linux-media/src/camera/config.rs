use caml_core::frontend::CaptureProfile;
use libcamera::pixel_format::PixelFormat;
use libcamera::stream::StreamConfiguration;

pub fn resolve_camera_index(input: &str) -> usize {
    input
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse::<usize>()
        .unwrap_or(0)
}

pub fn apply_capture_profile(cfg: &mut StreamConfiguration, profile: &CaptureProfile) {
    cfg.size.width = profile.width;
    cfg.size.height = profile.height;

    if profile.pixel_format.len() == 4 {
        let b = profile.pixel_format.as_bytes();
        cfg.pixel_format = PixelFormat::from_fourcc([b[0], b[1], b[2], b[3]]);
    }
}
