use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use libcamera::{
    camera::CameraConfigurationStatus,
    camera_manager::CameraManager,
    framebuffer_allocator::{FrameBuffer, FrameBufferAllocator},
    framebuffer_map::MemoryMappedFrameBuffer,
    pixel_format::PixelFormat,
    properties,
    stream::StreamRole,
};

use caml_core::{CompiledPipeline, MediaPayload, PipelineContext, RuntimeError};

use crate::{LibcameraFrameProvider, LibcameraProviderFactory};

#[derive(Clone, Default)]
pub struct NativeLibcameraFactory;

#[async_trait::async_trait]
impl LibcameraProviderFactory for NativeLibcameraFactory {
    async fn open(
        &self,
        pipeline: &CompiledPipeline,
    ) -> Result<Box<dyn LibcameraFrameProvider>, RuntimeError> {
        let mgr = CameraManager::new().map_err(|e| {
            RuntimeError::adapter(format!(
                "failed to initialize libcamera CameraManager: {:?}",
                e
            ))
        })?;
        let cameras = mgr.cameras();
        if cameras.is_empty() {
            return Err(RuntimeError::adapter(
                "no libcamera compatible cameras found",
            ));
        }

        let cam = cameras
            .get(0)
            .ok_or_else(|| RuntimeError::adapter("camera not found"))?;

        let mut cam = cam
            .acquire()
            .map_err(|e| RuntimeError::adapter(format!("failed to acquire camera: {:?}", e)))?;

        let mut cfgs = cam
            .generate_configuration(&[StreamRole::VideoRecording])
            .map_err(|e| {
                RuntimeError::adapter(format!("failed to generate configuration: {:?}", e))
            })?;

        let cfg = cfgs.get_mut(0).unwrap();

        // Map pixel format if capture profile is available
        // Note: For now we'll rely on default configuration but will allow future format parsing
        if let Some(processing) = &pipeline.processing {
            // Future feature: map processing or capture profile exactly
        }

        let status = cfgs.validate();
        match status {
            CameraConfigurationStatus::Invalid => {
                return Err(RuntimeError::adapter("invalid libcamera configuration"))
            }
            _ => {}
        }

        cam.configure(&mut cfgs)
            .map_err(|e| RuntimeError::adapter(format!("failed to configure camera: {:?}", e)))?;

        let mut alloc = FrameBufferAllocator::new(&cam);
        let stream = cfgs.get(0).unwrap().stream().unwrap();
        let buffers = alloc.alloc(&stream).map_err(|e| {
            RuntimeError::adapter(format!("failed to allocate frame buffers: {:?}", e))
        })?;

        let mapped_buffers = buffers
            .into_iter()
            .map(|buf| MemoryMappedFrameBuffer::new(buf).unwrap())
            .collect::<Vec<_>>();

        let mut reqs = mapped_buffers
            .into_iter()
            .map(|buf| {
                let mut req = cam.create_request(None).unwrap();
                req.add_buffer(&stream, buf).unwrap();
                req
            })
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::unbounded_channel();

        cam.on_request_completed(move |req| {
            let _ = tx.send(req);
        });

        cam.start(None)
            .map_err(|e| RuntimeError::adapter(format!("failed to start camera: {:?}", e)))?;

        for req in reqs {
            cam.queue_request(req).map_err(|e| {
                RuntimeError::adapter(format!("failed to queue request: {:?}", e.1))
            })?;
        }

        Ok(Box::new(NativeLibcameraProvider {
            camera: Arc::new(cam),
            receiver: rx,
            stream: stream,
        }))
    }
}

pub struct NativeLibcameraProvider {
    camera: Arc<libcamera::camera::ActiveCamera<'static>>,
    receiver: mpsc::UnboundedReceiver<libcamera::request::Request>,
    stream: libcamera::stream::Stream,
}

#[async_trait::async_trait]
impl LibcameraFrameProvider for NativeLibcameraProvider {
    async fn next_payload(
        &mut self,
        context: &mut PipelineContext,
    ) -> Result<MediaPayload, RuntimeError> {
        let mut req = self
            .receiver
            .recv()
            .await
            .ok_or_else(|| RuntimeError::adapter("camera receiver closed"))?;

        let mut data = context.acquire_buffer();
        {
            let framebuffer: &MemoryMappedFrameBuffer<FrameBuffer> =
                req.buffer(&self.stream).unwrap();
            for plane in framebuffer.data() {
                data.extend_from_slice(plane);
            }
        }

        req.reuse(libcamera::request::ReuseFlag::ReuseBuffers);
        let active_cam: &mut libcamera::camera::ActiveCamera<'static> =
            unsafe { &mut *(Arc::as_ptr(&self.camera) as *mut _) };
        active_cam
            .queue_request(req)
            .map_err(|e| RuntimeError::adapter(format!("failed to requeue request: {:?}", e.1)))?;

        Ok(MediaPayload::EncodedPacket(caml_core::EncodedPacket {
            codec: "raw".to_string(),
            timestamp: Some(Duration::from_millis(0)),
            duration: None,
            is_keyframe: true,
            data,
        }))
    }
}
