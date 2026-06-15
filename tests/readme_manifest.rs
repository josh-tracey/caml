//! Verify that the README manifest example parses correctly and includes WebRTC outputs.

use caml::{CamlManifest, OutputProfile};

#[test]
fn readme_manifest_parses_with_webrtc_output() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_5"
  cma_allocation_limit: "512MB"

pipelines:
  - id: "forward_primary"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
    outputs:
      - type: "webrtc_rtp"
        codec: "h264"
        payload_type: 102
        mtu: 1200
        ssrc: "stable"
        clock_rate: 90000

  - id: "belly_optical"
    input: "/dev/video0"
    type: "device"
    strategy: "transcode"
    backend: "v4l2"
    processing:
      codec: "h264"
      encoder: "software"
      preset: "ultrafast"
      tune: "zerolatency"
      frame_rate: 30
      bitrate: "512k"
      rotation: 90
"#,
    )
    .expect("README manifest should parse");

    assert_eq!(manifest.pipelines.len(), 2);

    // First pipeline must have a WebRTC output
    let forward = &manifest.pipelines[0];
    assert!(
        forward.outputs.iter().any(|o| matches!(o, OutputProfile::WebrtcRtp { .. })),
        "forward_primary must have a WebrtcRtp output"
    );

    // Second pipeline has no outputs (device transcode)
    let belly = &manifest.pipelines[1];
    assert!(
        belly.outputs.is_empty(),
        "belly_optical should have no explicit outputs"
    );
}
