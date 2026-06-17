use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use caml_core::{
    CodecPath, CompiledOverlayProfile, CompiledPipeline, CompiledProcessingProfile, InputType,
    ResolvedInputBackend, RuntimeError, StreamStrategy, Transport,
};

use crate::capabilities::init_ffmpeg;
use crate::hwaccel::{transcode_backend_for_pipeline, TranscodeBackend};
use crate::device::{
    open_media_input, best_video_stream, codec_name, frame_duration_from_rate,
    normalize_encoded_packet, duration_from_time_base, duration_from_packet,
    frame_duration_from_processing,
};
use crate::h264::extract_h264_config;


pub const DEFAULT_FRAME_DURATION: Duration = Duration::from_millis(33);

#[derive(Debug, Clone)]
pub struct WorkerSpec {
    pub pipeline_id: String,
    pub input: String,
    pub input_spec: InputSpec,
    pub mode: WorkerMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSpec {
    Rtsp {
        transport: Transport,
    },
    Device {
        backend: ResolvedInputBackend,
        frame_rate: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerMode {
    Passthrough {
        default_frame_duration: Duration,
    },
    Transcode {
        processing: CompiledProcessingProfile,
        overlay: Option<CompiledOverlayProfile>,
        backend: TranscodeBackend,
        default_frame_duration: Duration,
    },
}

#[derive(Debug)]
pub enum WorkerMessage {
    Packet(OwnedEncodedPacket),
    EndOfStream,
    RecoverableError(String),
    Error(String),
}

#[derive(Debug)]
pub struct OwnedEncodedPacket {
    pub codec: String,
    pub timestamp: Option<Duration>,
    pub duration: Option<Duration>,
    pub is_keyframe: bool,
    pub data: Vec<u8>,
}

pub fn run_worker(
    spec: WorkerSpec,
    tx: mpsc::Sender<WorkerMessage>,
    cancel: CancellationToken,
) -> Result<(), RuntimeError> {
    if !init_ffmpeg() {
        let _ = tx.blocking_send(WorkerMessage::Error(
            "ffmpeg initialization failed; verify FFmpeg libraries are available".to_string(),
        ));
        return Ok(());
    }

    let is_local_file = !spec.input.starts_with("rtsp://") 
        && !spec.input.starts_with("rtmp://")
        && !spec.input.starts_with("http://")
        && !spec.input.starts_with("https://")
        && !spec.input.starts_with("/dev/video");

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let result = match &spec.mode {
            WorkerMode::Passthrough {
                default_frame_duration,
            } => demux_packets(&spec.input, &spec.input_spec, *default_frame_duration, &tx, &cancel, is_local_file),
            WorkerMode::Transcode {
                processing,
                overlay,
                backend,
                default_frame_duration,
            } => crate::transcode::transcode_packets(crate::transcode::TranscodeJob {
                input: &spec.input,
                input_spec: &spec.input_spec,
                processing,
                overlay: overlay.as_ref(),
                backend: *backend,
                default_frame_duration: *default_frame_duration,
                tx: &tx,
                cancel: &cancel,
                is_local_file,
            }),
        };

        if let Err(ref error) = result {
            let _ = tx.blocking_send(WorkerMessage::RecoverableError(format!(
                "pipeline '{}' failed during FFmpeg execution: {}",
                spec.pipeline_id, error
            )));
            // Sleep to avoid tight loop in case of persistent error
            std::thread::sleep(Duration::from_secs(1));
        }

        if !is_local_file {
            match result {
                Ok(()) => {
                    let _ = tx.blocking_send(WorkerMessage::EndOfStream);
                    break;
                }
                Err(ref error) => {
                    println!("caml-ffmpeg: Live stream connection lost ({}), retrying in 2 seconds...", error);
                    std::thread::sleep(Duration::from_secs(2));
                    continue;
                }
            }
        }

        println!("caml-ffmpeg: Looping local file input '{}'", spec.input);
    }

    Ok(())
}

pub fn demux_packets(
    input: &str,
    input_spec: &InputSpec,
    default_frame_duration: Duration,
    tx: &mpsc::Sender<WorkerMessage>,
    cancel: &CancellationToken,
    is_local_file: bool,
) -> Result<(), String> {
    let mut context = open_media_input(input, input_spec)?;
    let (stream_index, time_base, codec_name_str, nominal_duration, h264_config) = {
        let stream = best_video_stream(&context, input)?;
        let codec_name_str = codec_name(stream.parameters().id())?;
        let h264_config = if codec_name_str == "h264" {
            extract_h264_config(&stream.parameters())?
        } else {
            None
        };

        (
            stream.index(),
            stream.time_base(),
            codec_name_str,
            frame_duration_from_rate(stream.avg_frame_rate()).unwrap_or(default_frame_duration),
            h264_config,
        )
    };

    let start_instant = std::time::Instant::now();
    let mut first_pts_duration = None;

    for (stream, packet) in context.packets() {
        if cancel.is_cancelled() {
            break;
        }

        if stream.index() != stream_index {
            continue;
        }

        if is_local_file {
            if let Some(pts) = packet.pts() {
                let pts_duration = duration_from_time_base(Some(pts), time_base).unwrap_or(Duration::ZERO);
                let first_pts = *first_pts_duration.get_or_insert(pts_duration);
                let relative_duration = pts_duration.checked_sub(first_pts).unwrap_or(Duration::ZERO);
                let target_time = start_instant + relative_duration;
                let now = std::time::Instant::now();
                if target_time > now {
                    std::thread::sleep(target_time - now);
                }
            }
        }

        let raw_data = packet
            .data()
            .ok_or_else(|| "ffmpeg returned a packet with no payload".to_string())?;
        let normalized =
            normalize_encoded_packet(codec_name_str, raw_data, packet.is_key(), h264_config.as_ref())?;

        let message = WorkerMessage::Packet(OwnedEncodedPacket {
            codec: codec_name_str.to_string(),
            timestamp: duration_from_time_base(packet.pts(), time_base),
            duration: duration_from_packet(packet.duration(), time_base).or(Some(nominal_duration)),
            is_keyframe: packet.is_key(),
            data: normalized,
        });

        if tx.blocking_send(message).is_err() {
            break;
        }
    }

    Ok(())
}

pub fn worker_spec_for_pipeline(pipeline: &CompiledPipeline) -> Result<WorkerSpec, RuntimeError> {
    let transport = pipeline
        .network
        .as_ref()
        .map(|profile| profile.transport)
        .unwrap_or(Transport::Tcp);

    let default_frame_duration = pipeline
        .processing
        .as_ref()
        .and_then(frame_duration_from_processing)
        .unwrap_or(DEFAULT_FRAME_DURATION);

    let input = match pipeline.input.kind {
        InputType::Rtsp => InputSpec::Rtsp { transport },
        InputType::Device => match pipeline.resolved_backend {
            ResolvedInputBackend::AutoDevice | ResolvedInputBackend::V4l2Device => {
                InputSpec::Device {
                    backend: pipeline.resolved_backend,
                    frame_rate: pipeline
                        .processing
                        .as_ref()
                        .map(|processing| processing.frame_rate)
                        .filter(|frame_rate| *frame_rate > 0),
                }
            }
            ResolvedInputBackend::LibcameraDevice => {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' selected a libcamera device backend, but the current FFmpeg adapter only implements RTSP and V4L2-backed device ingest",
                    pipeline.id
                )));
            }
            ResolvedInputBackend::FfmpegRtsp => {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' resolved an invalid FFmpeg RTSP backend for device input",
                    pipeline.id
                )));
            }
        },
    };

    let mode = match pipeline.strategy {
        StreamStrategy::Passthrough => WorkerMode::Passthrough {
            default_frame_duration,
        },
        StreamStrategy::Transcode => {
            let processing = pipeline.processing.clone().ok_or_else(|| {
                RuntimeError::adapter(format!(
                    "pipeline '{}' was compiled for transcode but has no processing profile",
                    pipeline.id
                ))
            })?;

            validate_transcode_support(pipeline, &processing)?;
            crate::transcode::validate_overlay_runtime(pipeline)
                .map_err(RuntimeError::adapter)?;
            let backend = transcode_backend_for_pipeline(pipeline)?;

            WorkerMode::Transcode {
                processing,
                overlay: pipeline.overlay.clone(),
                backend,
                default_frame_duration,
            }
        }
        StreamStrategy::HardwareDecode => {
            let processing = pipeline.processing.clone().ok_or_else(|| {
                RuntimeError::adapter(format!(
                    "pipeline '{}' was compiled for hardware decode but has no processing profile",
                    pipeline.id
                ))
            })?;

            validate_transcode_support(pipeline, &processing)?;
            crate::transcode::validate_overlay_runtime(pipeline)
                .map_err(RuntimeError::adapter)?;
            let backend = transcode_backend_for_pipeline(pipeline)?;

            WorkerMode::Transcode {
                processing,
                overlay: pipeline.overlay.clone(),
                backend,
                default_frame_duration,
            }
        }
    };

    Ok(WorkerSpec {
        pipeline_id: pipeline.id.clone(),
        input: pipeline.input.source.clone(),
        input_spec: input,
        mode,
    })
}

pub fn validate_transcode_support(
    pipeline: &CompiledPipeline,
    processing: &CompiledProcessingProfile,
) -> Result<(), RuntimeError> {
    let encoder = processing.encoder.trim().to_ascii_lowercase();
    match pipeline.codec_path {
        CodecPath::SoftwareTranscode | CodecPath::HardwareDecode => {
            if !matches!(encoder.as_str(), "software" | "libx264") {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' requested encoder '{}' but the current FFmpeg backend only implements software H.264 encode for this pipeline",
                    pipeline.id, processing.encoder
                )));
            }
        }
        CodecPath::HardwareTranscode => {
            if !matches!(encoder.as_str(), "hardware" | "v4l2m2m") {
                return Err(RuntimeError::adapter(format!(
                    "pipeline '{}' requested encoder '{}' but Raspberry Pi hardware transcode requires 'hardware' or 'v4l2m2m'",
                    pipeline.id, processing.encoder
                )));
            }
        }
        CodecPath::Passthrough => {
            return Err(RuntimeError::adapter(format!(
                "pipeline '{}' cannot request transcode support while compiled for passthrough",
                pipeline.id
            )));
        }
    }

    let codec = processing.codec.trim().to_ascii_lowercase();
    if codec != "h264" {
        return Err(RuntimeError::adapter(format!(
            "pipeline '{}' requested codec '{}' but the current FFmpeg backend only implements software H.264 transcode",
            pipeline.id, processing.codec
        )));
    }

    frame_duration_from_processing(processing).ok_or_else(|| {
        RuntimeError::adapter(format!(
            "pipeline '{}' must set a positive frame_rate for software transcode",
            pipeline.id
        ))
    })?;

    crate::transcode::normalize_rotation(processing.rotation).map_err(RuntimeError::adapter)?;

    Ok(())
}
