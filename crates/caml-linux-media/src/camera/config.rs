use caml_core::frontend::CaptureProfile;
use libcamera::pixel_format::PixelFormat;
use libcamera::stream::StreamConfiguration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CameraSelector {
    Index(usize),
    Id(String),
    Model(String),
}

impl CameraSelector {
    pub fn parse(input: &str) -> Self {
        if let Some(rest) = input.strip_prefix("camera:") {
            if let Ok(idx) = rest.parse::<usize>() {
                return Self::Index(idx);
            }
        } else if let Some(rest) = input.strip_prefix("libcamera:id:") {
            return Self::Id(rest.to_string());
        } else if let Some(rest) = input.strip_prefix("libcamera:model:") {
            return Self::Model(rest.to_string());
        }

        let digits = input
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();
        if let Ok(idx) = digits.parse::<usize>() {
            Self::Index(idx)
        } else {
            Self::Index(0)
        }
    }
}

pub fn apply_capture_profile(cfg: &mut StreamConfiguration, profile: &CaptureProfile) {
    cfg.size.width = profile.width;
    cfg.size.height = profile.height;

    if profile.pixel_format.len() == 4 {
        let b = profile.pixel_format.as_bytes();
        cfg.pixel_format = PixelFormat::from_fourcc([b[0], b[1], b[2], b[3]]);
    }
}
