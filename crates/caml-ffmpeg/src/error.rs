
use caml_core::RuntimeError;
use ffmpeg_next::Error as FfmpegError;

#[derive(Debug)]
pub enum FfmpegErrorClass {
    NetworkStall,
    DeviceLost,
    InvalidData,
    CodecError,
    Unknown,
}

impl FfmpegErrorClass {
    pub fn from_ffmpeg_error(error: &FfmpegError) -> Self {
        match error {
            FfmpegError::Eof => Self::DeviceLost,
            FfmpegError::Bug | FfmpegError::Bug2 => Self::CodecError,
            FfmpegError::InvalidData => Self::InvalidData,
            FfmpegError::StreamNotFound => Self::InvalidData,
            // Timeout/EAGAIN can mean network stall
            FfmpegError::Other { .. } => Self::Unknown,
            _ => Self::Unknown,
        }
    }
    
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::NetworkStall | Self::DeviceLost | Self::InvalidData)
    }
}

pub fn map_ffmpeg_error(ctx: &str, error: FfmpegError) -> RuntimeError {
    let class = FfmpegErrorClass::from_ffmpeg_error(&error);
    if class.is_recoverable() {
        RuntimeError::recoverable(format!("{} (recoverable {:?}): {}", ctx, class, error))
    } else {
        RuntimeError::adapter(format!("{} ({:?}): {}", ctx, class, error))
    }
}
