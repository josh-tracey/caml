pub mod adapters;
mod builder;

pub use builder::{RuntimeBuilder, RuntimeBuilderError};
pub use caml_core::*;

pub mod compiler {
    pub use caml_core::compiler::*;
}

pub mod error {
    pub use caml_core::error::*;
}

pub mod frontend {
    pub use caml_core::frontend::*;
}

pub mod runtime {
    pub use caml_core::runtime::*;
}

pub mod units {
    pub use caml_core::units::*;
}

#[cfg(feature = "ffmpeg")]
pub use caml_ffmpeg as ffmpeg;

#[cfg(feature = "pi")]
pub use caml_linux_media as linux_media;

#[cfg(feature = "webrtc")]
pub use caml_webrtc as webrtc;
