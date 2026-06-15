fn extract_h264_config(
    parameters: &ffmpeg::codec::Parameters,
) -> Result<Option<H264Config>, String> {
    let (extradata, size) = unsafe {
        let ptr = parameters.as_ptr();
        let extradata_ptr = (*ptr).extradata;
        let extradata_size = (*ptr).extradata_size;
        if extradata_ptr.is_null() || extradata_size <= 0 {
            return Ok(None);
        }

        (
            std::slice::from_raw_parts(extradata_ptr, extradata_size as usize),
            extradata_size as usize,
        )
    };

    if size == 0 {
        return Ok(None);
    }

    if looks_like_annex_b(extradata) {
        return Ok(Some(H264Config {
            nalu_length_size: 4,
            parameter_sets_annexb: extradata.to_vec(),
        }));
    }

    if extradata[0] != 1 {
        return Ok(None);
    }

    let (nalu_length_size, parameter_sets_annexb) = parse_avcc_extradata(extradata)?;
    Ok(Some(H264Config {
        nalu_length_size,
        parameter_sets_annexb,
    }))
}

fn parse_avcc_extradata(extradata: &[u8]) -> Result<(usize, Vec<u8>), String> {
    if extradata.len() < 7 {
        return Err("AVCC extradata is too short to parse".to_string());
    }

    let nalu_length_size = ((extradata[4] & 0b11) + 1) as usize;
    let mut cursor = 5usize;
    let sps_count = (extradata[cursor] & 0b0001_1111) as usize;
    cursor += 1;

    let mut parameter_sets_annexb = Vec::new();
    for _ in 0..sps_count {
        let (next, unit) = read_avcc_unit(extradata, cursor)?;
        cursor = next;
        parameter_sets_annexb.extend_from_slice(H264_START_CODE);
        parameter_sets_annexb.extend_from_slice(unit);
    }

    if cursor >= extradata.len() {
        return Ok((nalu_length_size, parameter_sets_annexb));
    }

    let pps_count = extradata[cursor] as usize;
    cursor += 1;
    for _ in 0..pps_count {
        let (next, unit) = read_avcc_unit(extradata, cursor)?;
        cursor = next;
        parameter_sets_annexb.extend_from_slice(H264_START_CODE);
        parameter_sets_annexb.extend_from_slice(unit);
    }

    Ok((nalu_length_size, parameter_sets_annexb))
}

fn read_avcc_unit(data: &[u8], cursor: usize) -> Result<(usize, &[u8]), String> {
    if cursor + 2 > data.len() {
        return Err("AVCC extradata ended before a NAL length field".to_string());
    }

    let size = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
    let start = cursor + 2;
    let end = start
        .checked_add(size)
        .ok_or_else(|| "AVCC NAL length overflowed".to_string())?;
    if end > data.len() {
        return Err("AVCC extradata contained a truncated NAL unit".to_string());
    }

    Ok((end, &data[start..end]))
}

fn normalize_h264_payload(
    payload: &[u8],
    is_keyframe: bool,
    config: Option<&H264Config>,
) -> Result<Vec<u8>, String> {
    let mut output = if looks_like_annex_b(payload) {
        payload.to_vec()
    } else {
        avcc_to_annex_b(
            payload,
            config.map(|value| value.nalu_length_size).unwrap_or(4),
        )?
    };

    if is_keyframe {
        if let Some(config) = config {
            let (has_sps, has_pps) = annex_b_parameter_sets(&output);
            if !has_sps || !has_pps {
                let mut prefixed =
                    Vec::with_capacity(config.parameter_sets_annexb.len() + output.len());
                prefixed.extend_from_slice(&config.parameter_sets_annexb);
                prefixed.extend_from_slice(&output);
                output = prefixed;
            }
        }
    }

    Ok(output)
}

fn avcc_to_annex_b(payload: &[u8], nalu_length_size: usize) -> Result<Vec<u8>, String> {
    if !(1..=4).contains(&nalu_length_size) {
        return Err(format!(
            "invalid AVCC NALU length size {}; expected 1-4 bytes",
            nalu_length_size
        ));
    }

    let mut cursor = 0usize;
    let mut output = Vec::with_capacity(payload.len() + 32);

    while cursor < payload.len() {
        if cursor + nalu_length_size > payload.len() {
            return Err("AVCC payload ended before a NALU size field".to_string());
        }

        let mut unit_length = 0usize;
        for byte in &payload[cursor..cursor + nalu_length_size] {
            unit_length = (unit_length << 8) | usize::from(*byte);
        }
        cursor += nalu_length_size;

        if unit_length == 0 {
            continue;
        }

        let end = cursor
            .checked_add(unit_length)
            .ok_or_else(|| "AVCC NALU length overflowed".to_string())?;
        if end > payload.len() {
            return Err("AVCC payload contained a truncated NALU".to_string());
        }

        output.extend_from_slice(H264_START_CODE);
        output.extend_from_slice(&payload[cursor..end]);
        cursor = end;
    }

    if output.is_empty() && !payload.is_empty() {
        return Err("AVCC payload produced no NAL units".to_string());
    }

    Ok(output)
}

fn looks_like_annex_b(payload: &[u8]) -> bool {
    payload.starts_with(H264_START_CODE) || payload.starts_with(&[0x00, 0x00, 0x01])
}

fn annex_b_parameter_sets(payload: &[u8]) -> (bool, bool) {
    let mut has_sps = false;
    let mut has_pps = false;
    let mut cursor = 0usize;

    while let Some((start, code_len)) = find_start_code(payload, cursor) {
        let nalu_start = start + code_len;
        let next = find_start_code(payload, nalu_start)
            .map(|(offset, _)| offset)
            .unwrap_or(payload.len());
        if nalu_start < next {
            let nalu_type = payload[nalu_start] & 0x1F;
            if nalu_type == 7 {
                has_sps = true;
            } else if nalu_type == 8 {
                has_pps = true;
            }
        }
        cursor = next;
    }

    (has_sps, has_pps)
}

fn find_start_code(payload: &[u8], offset: usize) -> Option<(usize, usize)> {
    let mut index = offset;
    while index + 3 <= payload.len() {
        if index + 4 <= payload.len() && payload[index..index + 4] == *H264_START_CODE {
            return Some((index, 4));
        }
        if payload[index..index + 3] == [0x00, 0x00, 0x01] {
            return Some((index, 3));
        }
        index += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use caml_core::{
        CodecPath, CompiledInput, CompiledPipeline, CompiledProcessingProfile, ExecutionMode,
        RecoveryPolicy, ResolvedInputBackend, RuntimeError, RuntimePolicy, SourceFactory,
        StreamStrategy,
    };

    use super::{
        frame_duration_from_processing, normalize_h264_payload, normalize_rotation,
        parse_avcc_extradata, rotated_dimensions, transcode_backend_for_pipeline,
        worker_spec_for_pipeline, FfmpegSourceFactory, H264Config, InputSpec, Rotation,
        TranscodeBackend, WorkerMode, H264_START_CODE,
    };

    #[test]
    fn parses_avcc_extradata_and_converts_payload_to_annex_b() {
        let extradata = vec![
            0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1F, 0x01, 0x00,
            0x04, 0x68, 0xEE, 0x3C, 0x80,
        ];
        let (nalu_length_size, parameter_sets_annexb) =
            parse_avcc_extradata(&extradata).expect("extradata should parse");
        assert_eq!(nalu_length_size, 4);
        assert_eq!(
            parameter_sets_annexb,
            vec![
                0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x01, 0x68, 0xEE,
                0x3C, 0x80
            ]
        );

        let payload = vec![0x00, 0x00, 0x00, 0x03, 0x65, 0x88, 0x84];
        let normalized = normalize_h264_payload(
            &payload,
            true,
            Some(&H264Config {
                nalu_length_size,
                parameter_sets_annexb,
            }),
        )
        .expect("payload should normalize");

        assert!(normalized.starts_with(H264_START_CODE));
        assert_eq!(
            &normalized[16..],
            &[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84]
        );
    }

    #[test]
    fn leaves_existing_annex_b_keyframe_intact_when_parameter_sets_are_present() {
        let payload = vec![
            0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x01, 0x68, 0xEE,
            0x3C, 0x80, 0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84,
        ];
        let normalized = normalize_h264_payload(&payload, true, None).expect("payload should pass");
        assert_eq!(normalized, payload);
    }

    #[test]
    fn normalizes_supported_rotation_values() {
        assert_eq!(normalize_rotation(Some(90)).unwrap(), Rotation::Clockwise);
        assert_eq!(
            normalize_rotation(Some(-90)).unwrap(),
            Rotation::CounterClockwise
        );
        assert_eq!(normalize_rotation(Some(180)).unwrap(), Rotation::HalfTurn);
        assert_eq!(normalize_rotation(None).unwrap(), Rotation::None);
        assert!(normalize_rotation(Some(45)).is_err());
    }

    #[test]
    fn swaps_dimensions_for_quarter_turns() {
        assert_eq!(rotated_dimensions(1920, 1080, Rotation::None), (1920, 1080));
        assert_eq!(
            rotated_dimensions(1920, 1080, Rotation::Clockwise),
            (1080, 1920)
        );
    }

    #[test]
    fn derives_transcode_frame_duration() {
        let processing = CompiledProcessingProfile {
            codec: "h264".to_string(),
            encoder: "software".to_string(),
            preset: "ultrafast".to_string(),
            tune: "zerolatency".to_string(),
            frame_rate: 25,
            bitrate_bps: 512_000,
            rotation: None,
        };
        assert_eq!(
            frame_duration_from_processing(&processing),
            Some(Duration::from_millis(40))
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_transcode_codec_in_factory() {
        let factory = FfmpegSourceFactory;
        let pipeline = compiled_pipeline("h265", "software", Some(90));
        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("unsupported codec should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error
            .to_string()
            .contains("only implements software H.264 transcode"));
    }

    #[tokio::test]
    async fn rejects_unsupported_rotation_in_factory() {
        let factory = FfmpegSourceFactory;
        let pipeline = compiled_pipeline("h264", "software", Some(45));
        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("unsupported rotation should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error.to_string().contains("unsupported rotation"));
    }

    #[test]
    fn plans_v4l2_device_capture_for_device_inputs() {
        let mut pipeline = compiled_pipeline("h264", "software", Some(90));
        pipeline.input.kind = InputType::Device;
        pipeline.input.source = "/dev/video0".to_string();
        pipeline.resolved_backend = ResolvedInputBackend::V4l2Device;

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        assert_eq!(
            spec.input_spec,
            InputSpec::Device {
                backend: ResolvedInputBackend::V4l2Device,
                frame_rate: Some(25),
            }
        );
    }

    #[test]
    fn selects_pi4_hardware_transcode_backend() {
        let mut pipeline = compiled_pipeline("h264", "v4l2m2m", None);
        pipeline.codec_path = CodecPath::HardwareTranscode;

        assert_eq!(
            transcode_backend_for_pipeline(&pipeline).expect("backend should resolve"),
            TranscodeBackend::Pi4HardwareEncode
        );

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        match spec.mode {
            WorkerMode::Transcode { backend, .. } => {
                assert_eq!(backend, TranscodeBackend::Pi4HardwareEncode);
            }
            other => panic!("expected transcode worker mode, got {:?}", other),
        }
    }

    #[test]
    fn selects_pi5_hardware_decode_backend() {
        let mut pipeline = compiled_pipeline("h264", "software", None);
        pipeline.strategy = StreamStrategy::HardwareDecode;
        pipeline.codec_path = CodecPath::HardwareDecode;

        assert_eq!(
            transcode_backend_for_pipeline(&pipeline).expect("backend should resolve"),
            TranscodeBackend::Pi5HardwareDecode
        );

        let spec = worker_spec_for_pipeline(&pipeline).expect("worker spec should compile");
        match spec.mode {
            WorkerMode::Transcode { backend, .. } => {
                assert_eq!(backend, TranscodeBackend::Pi5HardwareDecode);
            }
            other => panic!("expected transcode worker mode, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rejects_libcamera_device_backend_in_ffmpeg_factory() {
        let factory = FfmpegSourceFactory;
        let mut pipeline = compiled_pipeline("h264", "software", None);
        pipeline.input.kind = InputType::Device;
        pipeline.input.source = "/dev/video0".to_string();
        pipeline.resolved_backend = ResolvedInputBackend::LibcameraDevice;

        let error = match factory.build_source(&pipeline).await {
            Ok(_) => panic!("libcamera backend should fail in ffmpeg adapter"),
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeError::Adapter(_)));
        assert!(error.to_string().contains("libcamera"));
    }

    fn compiled_pipeline(codec: &str, encoder: &str, rotation: Option<i32>) -> CompiledPipeline {
        CompiledPipeline {
            id: "camera_a".to_string(),
            input: CompiledInput {
                kind: InputType::Rtsp,
                source: "rtsp://127.0.0.1:8554/live".to_string(),
            },
            strategy: StreamStrategy::Transcode,
            network: Some(caml_core::CompiledNetworkProfile {
                transport: Transport::Tcp,
                packet_size_limit: 1200,
                stall_timeout: Duration::from_secs(1),
            }),
            processing: Some(CompiledProcessingProfile {
                codec: codec.to_string(),
                encoder: encoder.to_string(),
                preset: "ultrafast".to_string(),
                tune: "zerolatency".to_string(),
                frame_rate: 25,
                bitrate_bps: 512_000,
                rotation,
            }),
            runtime: RuntimePolicy {
                buffer_size: 1200,
                watchdog_timeout: Duration::from_secs(1),
            },
            resolved_backend: ResolvedInputBackend::FfmpegRtsp,
            execution_mode: ExecutionMode::DecodedFrames,
            codec_path: CodecPath::SoftwareTranscode,
            recovery: RecoveryPolicy {
                max_restarts: 0,
                restart_backoff: Duration::from_millis(1),
                class: caml_core::RecoveryClass::Network,
            },
            capability_requirements: Vec::new(),
            outputs: Vec::new(),
        }
    }

    use std::time::Duration;

    use caml_core::{InputType, Transport};
}
