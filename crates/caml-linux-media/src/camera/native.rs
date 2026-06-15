#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

use libcamera::{
    camera::CameraConfigurationStatus,
    camera_manager::CameraManager,
    framebuffer_allocator::{FrameBuffer, FrameBufferAllocator},
    framebuffer_map::MemoryMappedFrameBuffer,
    stream::StreamRole,
};

use caml_core::{
    frontend::CaptureProfile, CompiledPipeline, DecodedFrame, MediaPayload, MediaStorage,
    PipelineContext, RuntimeError,
};

use crate::{camera::error::CameraError, LibcameraFrameProvider, LibcameraProviderFactory};

use super::config::{apply_capture_profile, resolve_camera_index};

#[derive(Clone, Default)]
pub struct NativeLibcameraFactory;

#[async_trait::async_trait]
impl LibcameraProviderFactory for NativeLibcameraFactory {
    async fn open(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn LibcameraFrameProvider>, RuntimeError> {
        let (frame_tx, frame_rx) = tokio_mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();

        // Extract capture profile if present
        let capture_profile = pipeline
            .outputs
            .first()
            .and_then(|output| match output {
                // If there is no explicit capture profile, we can try to resolve it from the processing profile.
                _ => None,
            })
            .or_else(|| {
                // Check if pipeline has a processing profile that we can use
                pipeline.processing.as_ref().map(|p| CaptureProfile {
                    width: 1920, // defaults
                    height: 1080,
                    pixel_format: "NV12".to_string(),
                    frame_rate: p.frame_rate,
                })
            })
            .unwrap_or(CaptureProfile {
                width: 1920,
                height: 1080,
                pixel_format: "NV12".to_string(),
                frame_rate: 30,
            });

        let input_path = pipeline.input.source.clone();
        let buffer_pool = caml_core::runtime::BufferPool::new(pipeline.runtime.buffer_size);

        // Spawn the dedicated camera thread
        let width = capture_profile.width;
        let height = capture_profile.height;
        let pixel_format = capture_profile.pixel_format.clone();

        let join_handle = std::thread::spawn(move || {
            if let Err(e) = run_camera_worker(
                &input_path,
                capture_profile,
                buffer_pool,
                cmd_rx,
                frame_tx,
            ) {
                log::error!("Libcamera worker thread exited with error: {:?}", e);
            }
        });

        Ok(Box::new(NativeLibcameraProvider {
            cmd_tx,
            frame_rx,
            width,
            height,
            pixel_format,
            _worker_handle: Some(join_handle),
        }))
    }
}

pub struct CameraFrameMessage {
    pub timestamp: Duration,
    pub data: caml_core::runtime::MediaBuffer,
}

enum WorkerCmd {
    RequestCompleted(libcamera::request::Request),
    Shutdown,
}

fn run_camera_worker(
    input_path: &str,
    profile: CaptureProfile,
    buffer_pool: caml_core::runtime::BufferPool,
    cmd_rx: std::sync::mpsc::Receiver<WorkerCmd>,
    frame_tx: tokio_mpsc::UnboundedSender<CameraFrameMessage>,
) -> Result<(), CameraError> {
    let mgr = CameraManager::new().map_err(|e| CameraError::ManagerInit(format!("{:?}", e)))?;
    let cameras = mgr.cameras();
    if cameras.is_empty() {
        return Err(CameraError::NoCameras);
    }

    let index = resolve_camera_index(input_path);
    let cam = cameras
        .get(index)
        .ok_or_else(|| CameraError::CameraNotFound(input_path.to_string()))?;

    let mut cam = cam
        .acquire()
        .map_err(|e| CameraError::AcquireFailed(format!("{:?}", e)))?;

    let mut cfgs = cam
        .generate_configuration(&[StreamRole::VideoRecording])
        .map_err(|e| CameraError::ConfigGenerateFailed(format!("{:?}", e)))?;

    let cfg = cfgs
        .get_mut(0)
        .ok_or(CameraError::InvalidConfig)?;

    apply_capture_profile(cfg, &profile);

    match cfgs.validate() {
        CameraConfigurationStatus::Invalid => return Err(CameraError::InvalidConfig),
        _ => {}
    }

    cam.configure(&mut cfgs)
        .map_err(|e| CameraError::ConfigureFailed(format!("{:?}", e)))?;

    let mut alloc = FrameBufferAllocator::new(&cam);
    let stream = cfgs.get(0).unwrap().stream().unwrap();
    let buffers = alloc
        .alloc(&stream)
        .map_err(|e| CameraError::AllocFailed(format!("{:?}", e)))?;

    let mapped_buffers = buffers
        .into_iter()
        .map(|buf| MemoryMappedFrameBuffer::new(buf).unwrap())
        .collect::<Vec<_>>();

    let reqs = mapped_buffers
        .into_iter()
        .map(|buf| {
            let mut req = cam.create_request(None).unwrap();
            req.add_buffer(&stream, buf).unwrap();
            req
        })
        .collect::<Vec<_>>();

    // Setup callback to send requests to worker loop
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    cam.on_request_completed(move |req| {
        let _ = completed_tx.send(req);
    });

    cam.start(None)
        .map_err(|e| CameraError::StartFailed(format!("{:?}", e)))?;

    for req in reqs {
        cam.queue_request(req)
            .map_err(|e| CameraError::QueueFailed(format!("{:?}", e.1)))?;
    }

    loop {
        // We poll both completed requests from callback and shutdown command.
        // In a standard synchronous loop, we block on completed request receiver
        // but periodically check for control commands, or we can use crossbeam select.
        // Since std::sync::mpsc does not support select directly, we use try_recv on control commands.
        if let Ok(WorkerCmd::Shutdown) = cmd_rx.try_recv() {
            let _ = cam.stop();
            break;
        }

        // Block on the next completed request from callback
        match completed_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(mut req) => {
                let mut data = buffer_pool.acquire();
                let timestamp = Duration::from_nanos(req.sequence()); // sequence or timestamp representation

                {
                    let framebuffer: &MemoryMappedFrameBuffer<FrameBuffer> =
                        req.buffer(&stream).unwrap();
                    for plane in framebuffer.data() {
                        data.extend_from_slice(plane);
                    }
                }

                if frame_tx.send(CameraFrameMessage { timestamp, data }).is_err() {
                    // Pipeline closed, shut down camera worker
                    let _ = cam.stop();
                    break;
                }

                req.reuse(libcamera::request::ReuseFlag::ReuseBuffers);
                cam.queue_request(req)
                    .map_err(|e| CameraError::QueueFailed(format!("{:?}", e.1)))?;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Timeout, loop around to check control commands
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                // Callback sender disconnected, error out
                let _ = cam.stop();
                break;
            }
        }
    }

    Ok(())
}

pub struct NativeLibcameraProvider {
    cmd_tx: std::sync::mpsc::Sender<WorkerCmd>,
    frame_rx: tokio_mpsc::UnboundedReceiver<CameraFrameMessage>,
    width: u32,
    height: u32,
    pixel_format: String,
    _worker_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for NativeLibcameraProvider {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(WorkerCmd::Shutdown);
        if let Some(handle) = self._worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[async_trait::async_trait]
impl LibcameraFrameProvider for NativeLibcameraProvider {
    async fn next_payload(
        &mut self,
        _context: &mut PipelineContext,
    ) -> Result<MediaPayload, RuntimeError> {
        let msg = self
            .frame_rx
            .recv()
            .await
            .ok_or_else(|| RuntimeError::adapter("camera worker thread terminated"))?;

        Ok(MediaPayload::DecodedFrame(DecodedFrame {
            pixel_format: self.pixel_format.clone(),
            width: self.width,
            height: self.height,
            timestamp: Some(msg.timestamp),
            data: MediaStorage::Pooled(msg.data),
        }))
    }
}
