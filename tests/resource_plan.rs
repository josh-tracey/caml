use caml::{CamlCompiler, CamlManifest, CompileError, ResourceWarning};

#[test]
fn test_media_memory_limit_parsing_and_compilation() {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "50MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: 10s
"#;

    let manifest = CamlManifest::from_yaml_str(manifest_str).expect("should parse");
    let compiled = CamlCompiler::compile(&manifest).expect("should compile");

    let resource_plan = compiled.resource_plan;
    assert_eq!(resource_plan.configured_limit_bytes, 50_000_000);
}

#[test]
fn test_resource_warning_high_memory_usage() {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "150MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1000000
      stall_timeout: 10s
"#;

    let manifest = CamlManifest::from_yaml_str(manifest_str).expect("should parse");
    let compiled = CamlCompiler::compile(&manifest).expect("should compile");

    let plan = compiled.resource_plan;
    assert!(plan.warnings.contains(&ResourceWarning::HighMemoryUsage));
}

#[test]
fn test_resource_limit_exceeded_error() {
    let manifest_str = r#"
system:
  hardware_target: "GENERIC_LINUX"
  media_memory_limit: "50MB"
pipelines:
  - id: "generic_pipeline"
    input: "rtsp://192.168.1.50:554/stream1"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1000000
      stall_timeout: 10s
"#;

    let manifest = CamlManifest::from_yaml_str(manifest_str).expect("should parse");
    let compiled = CamlCompiler::compile(&manifest);
    match compiled {
        Err(CompileError::ResourceLimitExceeded {
            configured_limit_bytes,
            estimated_usage_bytes,
        }) => {
            assert_eq!(configured_limit_bytes, 50_000_000);
            assert_eq!(estimated_usage_bytes, 100_000_000);
        }
        other => panic!("expected ResourceLimitExceeded error, got {:?}", other),
    }
}
