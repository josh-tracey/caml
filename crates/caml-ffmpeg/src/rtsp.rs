fn open_rtsp_input(
    input: &str,
    transport: Transport,
) -> Result<ffmpeg::format::context::Input, String> {
    let mut options = ffmpeg::Dictionary::new();
    options.set(
        "rtsp_transport",
        match transport {
            Transport::Tcp => "tcp",
            Transport::Udp => "udp",
        },
    );
    options.set("fflags", "nobuffer");
    options.set("flags", "low_delay");

    ffmpeg::format::input_with_dictionary(&input, options)
        .map_err(|error| format!("unable to open RTSP input '{}': {}", input, error))
}

