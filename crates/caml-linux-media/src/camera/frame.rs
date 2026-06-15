use std::time::Duration;

use caml_core::runtime::MediaBuffer;

pub struct LibcameraFrame {
    pub timestamp: Duration,
    pub data: MediaBuffer,
}
