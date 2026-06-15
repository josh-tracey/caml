use caml_core::{CodecPath, CompiledPipeline};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodeBackend {
    Software,
    Pi4HardwareEncode,
    Pi5HardwareDecode,
}

pub fn transcode_backend_for_pipeline(
    pipeline: &CompiledPipeline,
) -> Result<TranscodeBackend, caml_core::RuntimeError> {
    match pipeline.codec_path {
        CodecPath::SoftwareTranscode => Ok(TranscodeBackend::Software),
        CodecPath::HardwareTranscode => Ok(TranscodeBackend::Pi4HardwareEncode),
        CodecPath::HardwareDecode => Ok(TranscodeBackend::Pi5HardwareDecode),
        CodecPath::Passthrough => Err(caml_core::RuntimeError::adapter(format!(
            "pipeline '{}' does not have a transcode codec path",
            pipeline.id
        ))),
    }
}
