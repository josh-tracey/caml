fn transcode_packets(
    input: &str,
    input_spec: &InputSpec,
    processing: &CompiledProcessingProfile,
    backend: TranscodeBackend,
    default_frame_duration: Duration,
    tx: &mpsc::Sender<WorkerMessage>,
) -> Result<(), String> {
    let mut context = open_media_input(input, input_spec)?;
    let nominal_duration =
        frame_duration_from_processing(processing).unwrap_or(default_frame_duration);
    let (stream_index, mut transcoder) = {
        let stream = best_video_stream(&context, input)?;
        (
            stream.index(),
            VideoTranscoder::new(&stream, processing, backend, nominal_duration)?,
        )
    };

    for (stream, packet) in context.packets() {
        if stream.index() != stream_index {
            continue;
        }

        transcoder.send_packet_to_decoder(&packet)?;
        transcoder.receive_decoded_frames(tx)?;
    }

    transcoder.send_eof_to_decoder()?;
    transcoder.receive_decoded_frames(tx)?;
    transcoder.flush_filter()?;
    transcoder.receive_filtered_frames(tx)?;
    transcoder.send_eof_to_encoder()?;
    transcoder.receive_encoded_packets(tx)?;

    Ok(())
}

struct VideoTranscoder {
    decoder: ffmpeg::decoder::Video,
    filter: ffmpeg::filter::Graph,
    encoder: ffmpeg::encoder::Video,
    encoder_time_base: ffmpeg::Rational,
    encoder_h264_config: Option<H264Config>,
    nominal_duration: Duration,
}

impl VideoTranscoder {
    fn new(
        stream: &ffmpeg::format::stream::Stream<'_>,
        processing: &CompiledProcessingProfile,
        backend: TranscodeBackend,
        nominal_duration: Duration,
    ) -> Result<Self, String> {
        let decoder = open_video_decoder(stream, backend)?;

        let rotation = normalize_rotation(processing.rotation)?;
        let (output_width, output_height) =
            rotated_dimensions(decoder.width(), decoder.height(), rotation);
        let filter = build_video_filter(&decoder, stream.time_base(), rotation)?;
        let encoder =
            open_h264_encoder(&decoder, processing, backend, output_width, output_height)?;
        let encoder_time_base = encoder.time_base();
        let encoder_h264_config = extract_h264_config(&ffmpeg::codec::Parameters::from(&encoder))?;

        Ok(Self {
            decoder,
            filter,
            encoder,
            encoder_time_base,
            encoder_h264_config,
            nominal_duration,
        })
    }

    fn send_packet_to_decoder(&mut self, packet: &ffmpeg::Packet) -> Result<(), String> {
        self.decoder
            .send_packet(packet)
            .map_err(|error| format!("failed to send packet to decoder: {}", error))
    }

    fn send_eof_to_decoder(&mut self) -> Result<(), String> {
        self.decoder
            .send_eof()
            .map_err(|error| format!("failed to send decoder EOF: {}", error))
    }

    fn send_eof_to_encoder(&mut self) -> Result<(), String> {
        self.encoder
            .send_eof()
            .map_err(|error| format!("failed to send encoder EOF: {}", error))
    }

    fn receive_decoded_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut decoded = ffmpeg::frame::Video::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let pts = decoded.timestamp().or(decoded.pts());
            decoded.set_pts(pts);
            let mut source = self
                .filter
                .get("in")
                .ok_or_else(|| "filter graph is missing the input node".to_string())?;
            source
                .source()
                .add(&decoded)
                .map_err(|error| format!("failed to push frame into filter graph: {}", error))?;
            self.receive_filtered_frames(tx)?;
        }

        Ok(())
    }

    fn flush_filter(&mut self) -> Result<(), String> {
        let mut source = self
            .filter
            .get("in")
            .ok_or_else(|| "filter graph is missing the input node".to_string())?;
        source
            .source()
            .flush()
            .map_err(|error| format!("failed to flush video filter graph: {}", error))
    }

    fn receive_filtered_frames(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut sink = self
            .filter
            .get("out")
            .ok_or_else(|| "filter graph is missing the output node".to_string())?;
        let mut filtered = ffmpeg::frame::Video::empty();

        while sink.sink().frame(&mut filtered).is_ok() {
            let pts = filtered.timestamp().or(filtered.pts());
            filtered.set_pts(pts);
            filtered.set_kind(ffmpeg::picture::Type::None);
            self.encoder
                .send_frame(&filtered)
                .map_err(|error| format!("failed to send frame to encoder: {}", error))?;
            self.receive_encoded_packets(tx)?;
        }

        Ok(())
    }

    fn receive_encoded_packets(&mut self, tx: &mpsc::Sender<WorkerMessage>) -> Result<(), String> {
        let mut encoded = ffmpeg::Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            let raw_data = encoded
                .data()
                .ok_or_else(|| "encoder returned a packet with no payload".to_string())?;
            let normalized = normalize_h264_payload(
                raw_data,
                encoded.is_key(),
                self.encoder_h264_config.as_ref(),
            )?;

            let message = WorkerMessage::Packet(OwnedEncodedPacket {
                codec: "h264".to_string(),
                timestamp: duration_from_time_base(encoded.pts(), self.encoder_time_base),
                duration: duration_from_packet(encoded.duration(), self.encoder_time_base)
                    .or(Some(self.nominal_duration)),
                is_keyframe: encoded.is_key(),
                data: normalized,
            });

            if tx.blocking_send(message).is_err() {
                break;
            }
        }

        Ok(())
    }
}

fn open_video_decoder(
    stream: &ffmpeg::format::stream::Stream<'_>,
    backend: TranscodeBackend,
) -> Result<ffmpeg::decoder::Video, String> {
    let context = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
        .map_err(|error| format!("failed to create decoder context: {}", error))?;

    match backend {
        TranscodeBackend::Software | TranscodeBackend::Pi4HardwareEncode => context
            .decoder()
            .video()
            .map_err(|error| format!("failed to open video decoder: {}", error)),
        TranscodeBackend::Pi5HardwareDecode => {
            let codec_name = match stream.parameters().id() {
                ffmpeg::codec::Id::H264 => "h264_v4l2request",
                ffmpeg::codec::Id::HEVC => "hevc_v4l2request",
                other => {
                    return Err(format!(
                        "Raspberry Pi 5 hardware decode only supports H.264/H.265 inputs; stream codec was '{:?}'",
                        other
                    ));
                }
            };

            let codec = ffmpeg::decoder::find_by_name(codec_name).ok_or_else(|| {
                format!(
                    "the local FFmpeg build does not expose the '{}' hardware decoder",
                    codec_name
                )
            })?;

            let mut decoder = context.decoder();
            decoder.set_packet_time_base(stream.time_base());
            decoder
                .open_as(codec)
                .and_then(|opened| opened.video())
                .map_err(|error| {
                    format!(
                        "failed to open Raspberry Pi 5 hardware decoder '{}': {}",
                        codec_name, error
                    )
                })
        }
    }
}

fn build_video_filter(
    decoder: &ffmpeg::decoder::Video,
    time_base: ffmpeg::Rational,
    rotation: Rotation,
) -> Result<ffmpeg::filter::Graph, String> {
    let mut filter = ffmpeg::filter::Graph::new();
    let aspect_ratio = sanitize_rational(decoder.aspect_ratio());
    let args = format!(
        "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
        decoder.width(),
        decoder.height(),
        decoder
            .format()
            .descriptor()
            .map(|descriptor| descriptor.name())
            .unwrap_or("yuv420p"),
        time_base.numerator(),
        time_base.denominator(),
        aspect_ratio.numerator(),
        aspect_ratio.denominator()
    );

    filter
        .add(
            &ffmpeg::filter::find("buffer")
                .ok_or_else(|| "FFmpeg 'buffer' filter is unavailable".to_string())?,
            "in",
            &args,
        )
        .map_err(|error| format!("failed to add buffer filter: {}", error))?;
    filter
        .add(
            &ffmpeg::filter::find("buffersink")
                .ok_or_else(|| "FFmpeg 'buffersink' filter is unavailable".to_string())?,
            "out",
            "",
        )
        .map_err(|error| format!("failed to add buffersink filter: {}", error))?;

    {
        let mut out = filter
            .get("out")
            .ok_or_else(|| "filter graph is missing the output node".to_string())?;
        out.set_pixel_format(ffmpeg::format::Pixel::YUV420P);
    }

    filter
        .output("in", 0)
        .and_then(|parser| parser.input("out", 0))
        .and_then(|parser| parser.parse(rotation.filter_spec()))
        .map_err(|error| format!("failed to parse filter graph: {}", error))?;
    filter
        .validate()
        .map_err(|error| format!("failed to validate filter graph: {}", error))?;

    Ok(filter)
}

fn open_h264_encoder(
    decoder: &ffmpeg::decoder::Video,
    processing: &CompiledProcessingProfile,
    backend: TranscodeBackend,
    output_width: u32,
    output_height: u32,
) -> Result<ffmpeg::encoder::Video, String> {
    let codec = match backend {
        TranscodeBackend::Software | TranscodeBackend::Pi5HardwareDecode => {
            ffmpeg::encoder::find_by_name("libx264")
                .or_else(|| ffmpeg::encoder::find(ffmpeg::codec::Id::H264))
                .ok_or_else(|| {
                    "no software H.264 encoder was found in the local FFmpeg build".to_string()
                })?
        }
        TranscodeBackend::Pi4HardwareEncode => {
            ffmpeg::encoder::find_by_name("h264_v4l2m2m").ok_or_else(|| {
                "the local FFmpeg build does not expose the 'h264_v4l2m2m' Raspberry Pi hardware encoder".to_string()
            })?
        }
    };

    let mut encoder = ffmpeg::codec::context::Context::new_with_codec(codec)
        .encoder()
        .video()
        .map_err(|error| format!("failed to create H.264 encoder context: {}", error))?;

    let frame_rate = processing.frame_rate as i32;
    encoder.set_width(output_width);
    encoder.set_height(output_height);
    encoder.set_aspect_ratio(decoder.aspect_ratio());
    encoder.set_format(ffmpeg::format::Pixel::YUV420P);
    encoder.set_frame_rate(Some((frame_rate, 1)));
    encoder.set_time_base((1, frame_rate));
    encoder.set_bit_rate(processing.bitrate_bps as usize);
    encoder.set_max_b_frames(0);
    encoder.set_gop(processing.frame_rate.saturating_mul(2));

    let mut options = ffmpeg::Dictionary::new();
    if !processing.preset.trim().is_empty() {
        options.set("preset", processing.preset.trim());
    }
    if !processing.tune.trim().is_empty() {
        options.set("tune", processing.tune.trim());
    }

    encoder.open_with(options).map_err(|error| match backend {
        TranscodeBackend::Software => format!("failed to open software H.264 encoder: {}", error),
        TranscodeBackend::Pi4HardwareEncode => format!(
            "failed to open Raspberry Pi 4 hardware H.264 encoder: {}",
            error
        ),
        TranscodeBackend::Pi5HardwareDecode => format!(
            "failed to open software H.264 encoder after Pi 5 hardware decode: {}",
            error
        ),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rotation {
    None,
    Clockwise,
    HalfTurn,
    CounterClockwise,
}

impl Rotation {
    fn filter_spec(self) -> &'static str {
        match self {
            Self::None => "null",
            Self::Clockwise => "transpose=clock",
            Self::HalfTurn => "hflip,vflip",
            Self::CounterClockwise => "transpose=cclock",
        }
    }
}

fn normalize_rotation(rotation: Option<i32>) -> Result<Rotation, String> {
    match rotation.unwrap_or(0) {
        0 => Ok(Rotation::None),
        90 | -270 => Ok(Rotation::Clockwise),
        180 | -180 => Ok(Rotation::HalfTurn),
        270 | -90 => Ok(Rotation::CounterClockwise),
        other => Err(format!(
            "unsupported rotation '{}'; supported values are 0, 90, 180, 270, -90, -180, and -270",
            other
        )),
    }
}

fn rotated_dimensions(width: u32, height: u32, rotation: Rotation) -> (u32, u32) {
    match rotation {
        Rotation::Clockwise | Rotation::CounterClockwise => (height, width),
        Rotation::None | Rotation::HalfTurn => (width, height),
    }
}

fn sanitize_rational(value: ffmpeg::Rational) -> ffmpeg::Rational {
    if value.numerator() <= 0 || value.denominator() <= 0 {
        ffmpeg::Rational(1, 1)
    } else {
        value
    }
}

fn open_media_input(
    input: &str,
    input_spec: &InputSpec,
) -> Result<ffmpeg::format::context::Input, String> {
    match input_spec {
        InputSpec::Rtsp { transport } => open_rtsp_input(input, *transport),
        InputSpec::Device {
            backend,
            frame_rate,
        } => open_device_input(input, *backend, *frame_rate),
    }
}

