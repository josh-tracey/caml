#![cfg(feature = "pi")]

use std::sync::Arc;
use std::time::{Duration, Instant};
use caml::{CamlManifest, CapabilityProbe, HardwareTarget, RuntimeBuilder, PiModel};

fn pi_host_tests_enabled() -> bool {
    std::env::var_os("CAML_PI_HOST_TESTS").is_some()
}

#[tokio::test]
async fn test_pi4_hardware_encode_flow() {
    if !pi_host_tests_enabled() {
        eprintln!(
            "skipping Pi 4 hardware encode flow test; set CAML_PI_HOST_TESTS=1 on Pi 4 hardware"
        );
        return;
    }

    let probe = caml_linux_media::linux_capability_probe();
    let capabilities = probe
        .capabilities(HardwareTarget::RaspberryPi4)
        .expect("Linux capability probe should run");

    if capabilities.pi_model != Some(PiModel::Pi4) {
        eprintln!("skipping Pi 4 hardware encode flow test: not running on Raspberry Pi 4 hardware");
        return;
    }

    if !capabilities.has_pi4_h264_encoder {
        eprintln!("skipping Pi 4 hardware encode flow test: Pi 4 hardware encoder (v4l2m2m) not available");
        return;
    }

    if !std::path::Path::new("/dev/video0").exists() {
        eprintln!("skipping Pi 4 hardware encode flow test: /dev/video0 does not exist");
        return;
    }

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  media_memory_limit: "512MB"
pipelines:
  - id: "pi4_encode_flow"
    input: "/dev/video0"
    type: "device"
    backend: "v4l2"
    strategy: "transcode"
    processing:
      codec: "h264"
      encoder: "v4l2m2m"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
    outputs:
      - type: "recording"
"#
    )
    .expect("manifest should parse");

    let recording_packets = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut adapters = caml::adapters::BuiltinAdapters::default();
    adapters.recording_packets = Some(recording_packets.clone());

    #[cfg(feature = "ffmpeg")]
    {
        adapters.ffmpeg_source = Some(Arc::new(caml_ffmpeg::FfmpegSourceFactory::new()));
    }
    #[cfg(all(feature = "pi", target_os = "linux"))]
    {
        adapters.libcamera_source = Some(Arc::new(
            caml_linux_media::LibcameraSourceFactory::new(Arc::new(caml_linux_media::camera::NativeLibcameraFactory)),
        ));
    }

    let builder = RuntimeBuilder::new()
        .with_manifest(manifest)
        .with_capability_probe(Arc::new(probe))
        .with_runtime_factory(adapters);

    let runtime = builder.start().await.expect("failed to start Pi 4 encode runtime");

    let start_time = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify status
    let status = runtime.status().await;
    let pipeline_status = status.pipeline("pi4_encode_flow");
    println!("Pipeline status after 2 seconds: {:?}", pipeline_status);
    
    // Ensure pipeline did not fail
    assert_ne!(pipeline_status, Some(caml::runtime::TaskStatus::Failed));

    let packets = recording_packets.lock().await.clone();
    println!("Recorded {} packets.", packets.len());

    runtime.shutdown().await.expect("shutdown failed");
    println!("Pi 4 hardware encode flow completed. Packets run in: {:?}", start_time.elapsed());
}
