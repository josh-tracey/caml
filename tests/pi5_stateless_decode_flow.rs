#![cfg(feature = "pi")]

use std::sync::Arc;
use std::time::Duration;
use caml::{CamlManifest, CapabilityProbe, HardwareTarget, RuntimeBuilder, PiModel};

fn pi_host_tests_enabled() -> bool {
    std::env::var_os("CAML_PI_HOST_TESTS").is_some()
}

#[tokio::test]
async fn test_pi5_stateless_decode_flow() {
    if !pi_host_tests_enabled() {
        eprintln!(
            "skipping Pi 5 stateless decode flow test; set CAML_PI_HOST_TESTS=1 on Pi 5 hardware"
        );
        return;
    }

    let probe = caml_linux_media::linux_capability_probe();
    let capabilities = probe
        .capabilities(HardwareTarget::RaspberryPi5)
        .expect("Linux capability probe should run");

    if capabilities.pi_model != Some(PiModel::Pi5) {
        eprintln!("skipping Pi 5 stateless decode flow test: not running on Raspberry Pi 5 hardware");
        return;
    }

    if !capabilities.has_pi5_stateless_decoder {
        eprintln!("skipping Pi 5 stateless decode flow test: Pi 5 stateless decoder not available");
        return;
    }

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  media_memory_limit: "512MB"
pipelines:
  - id: "pi5_decode_flow"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "hardware_decode"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    processing:
      codec: "h264"
      encoder: "software"
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
    let mut adapters = caml::adapters::BuiltinAdapters {
        recording_packets: Some(recording_packets.clone()),
        ..Default::default()
    };

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

    let runtime = builder.start().await.expect("failed to start Pi 5 decode runtime");

    // Run for a bit
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify status
    let status = runtime.status().await;
    let pipeline_status = status.pipeline("pi5_decode_flow");
    println!("Pipeline status after 2 seconds: {:?}", pipeline_status);

    // Ensure pipeline did not fail
    assert_ne!(pipeline_status, Some(caml::runtime::TaskStatus::Failed));

    let packets = recording_packets.lock().await.clone();
    println!("Recorded {} packets.", packets.len());

    runtime.shutdown().await.expect("shutdown failed");
}
