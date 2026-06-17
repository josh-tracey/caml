use std::collections::HashMap;
use std::sync::Arc;

use caml::runtime::mock::{
    MockSinkFactory, MockSinkRecorder, MockSourceAction, MockSourceFactory, MockSourcePlan,
};
use caml::{CamlCompiler, CamlManifest, CompileError, RuntimeAdapters, RuntimeBuilder};

#[test]
fn test_generic_linux_compilation() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile(&manifest);
    assert!(compiled.is_ok());

    let builder = RuntimeBuilder::from_manifest(manifest).compile();
    assert!(builder.is_ok());
}

#[test]
fn test_pi4_compilation_without_probe_fails() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "pi4_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile(&manifest);
    assert!(matches!(
        compiled,
        Err(CompileError::InvalidConfiguration(_))
    ));
}

#[test]
fn test_pi4_compilation_unchecked_succeeds() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "RASPBERRY_PI_4"
  cma_allocation_limit: "256MB"
pipelines:
  - id: "pi4_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile_unchecked(&manifest);
    assert!(compiled.is_ok());
}

#[tokio::test]
async fn test_mock_pipeline_with_webrtc_outputs() {
    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "webrtc_pipeline"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 200ms
    outputs:
      - type: "webrtc_rtp"
        codec: "h264"
        payload_type: 102
        mtu: 1200
        clock_rate: 90000
"#,
    )
    .unwrap();

    let source_plan = MockSourcePlan::new(vec![
        MockSourceAction::Packet(vec![1, 2, 3]),
        MockSourceAction::EndOfStream,
    ]);
    let source_factory = Arc::new(MockSourceFactory::new(HashMap::from([(
        "webrtc_pipeline".to_string(),
        source_plan,
    )])));

    let recorder = MockSinkRecorder::default();
    let sink_factory = Arc::new(MockSinkFactory::new(HashMap::from([(
        "webrtc_pipeline".to_string(),
        recorder.clone(),
    )])));

    let adapters = RuntimeAdapters::new(source_factory, sink_factory);
    let handle = RuntimeBuilder::from_manifest(manifest)
        .with_runtime_factory(adapters)
        .start()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let payloads = recorder.payloads().await;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0], vec![1, 2, 3]);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_builtin_adapters_null_sink_when_no_outputs() {
    use caml::adapters::BuiltinAdapters;
    use caml::runtime::PipelineFactory;

    let manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "test_pipeline"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#,
    )
    .unwrap();
    let compiled = CamlCompiler::compile(&manifest).unwrap();

    let adapters = BuiltinAdapters::default();
    let res = adapters.build_pipeline(&compiled.pipelines[0]).await;
    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(
        err_msg.contains("ffmpeg feature is disabled")
            || err_msg.contains("ffmpeg source factory not configured")
    );
}
