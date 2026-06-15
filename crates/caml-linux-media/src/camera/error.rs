use caml_core::RuntimeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CameraError {
    #[error("failed to initialize libcamera CameraManager: {0}")]
    ManagerInit(String),
    #[error("no libcamera compatible cameras found")]
    NoCameras,
    #[error("camera not found: {0}")]
    CameraNotFound(String),
    #[error("failed to acquire camera: {0}")]
    AcquireFailed(String),
    #[error("failed to generate configuration: {0}")]
    ConfigGenerateFailed(String),
    #[error("invalid libcamera configuration")]
    InvalidConfig,
    #[error("failed to configure camera: {0}")]
    ConfigureFailed(String),
    #[error("failed to allocate frame buffers: {0}")]
    AllocFailed(String),
    #[error("failed to start camera: {0}")]
    StartFailed(String),
    #[error("failed to queue request: {0}")]
    QueueFailed(String),
    #[error("camera worker thread panicked or died")]
    WorkerDied,
    #[error("camera receiver closed")]
    ReceiverClosed,
}

impl From<CameraError> for RuntimeError {
    fn from(err: CameraError) -> Self {
        RuntimeError::adapter(err.to_string())
    }
}
