use ffmpeg_next as ffmpeg;

pub const H264_START_CODE: &[u8] = &[0x00, 0x00, 0x00, 0x01];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct H264Config {
    pub nalu_length_size: usize,
    pub parameter_sets_annexb: Vec<u8>,
}

pub fn extract_h264_config(
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

pub fn parse_avcc_extradata(extradata: &[u8]) -> Result<(usize, Vec<u8>), String> {
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

pub fn read_avcc_unit(data: &[u8], cursor: usize) -> Result<(usize, &[u8]), String> {
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

pub fn normalize_h264_payload(
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

pub fn avcc_to_annex_b(payload: &[u8], nalu_length_size: usize) -> Result<Vec<u8>, String> {
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

pub fn looks_like_annex_b(payload: &[u8]) -> bool {
    payload.starts_with(H264_START_CODE) || payload.starts_with(&[0x00, 0x00, 0x01])
}

pub fn annex_b_parameter_sets(payload: &[u8]) -> (bool, bool) {
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

pub fn find_start_code(payload: &[u8], offset: usize) -> Option<(usize, usize)> {
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
