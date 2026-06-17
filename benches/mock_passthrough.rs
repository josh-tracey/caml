use caml::{
    runtime::mock::{MockSinkRecorder, MockSourceAction, MockSourcePlan},
    CamlCompiler, CamlManifest, RuntimeBuilder,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn bench_mock_passthrough(c: &mut Criterion) {
    let runtime_manifest = CamlManifest::from_yaml_str(
        r#"
system:
  hardware_target: "GENERIC_LINUX"
  cma_allocation_limit: "128MB"
pipelines:
  - id: "bench_pipeline"
    input: "rtsp://127.0.0.1:8554/live"
    type: "rtsp"
    strategy: "passthrough"
    network:
      transport: "tcp"
      packet_size_limit: 1200
      stall_timeout: "500ms"
"#,
    )
    .unwrap();

    let compiled = CamlCompiler::compile(&runtime_manifest).unwrap();

    c.bench_function("mock_passthrough_throughput", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                // Send 20 packets through the mock pipeline
                let mut actions = Vec::new();
                for _ in 0..20 {
                    actions.push(MockSourceAction::Packet(vec![0u8; 1000]));
                }
                actions.push(MockSourceAction::EndOfStream);

                let plans =
                    HashMap::from([("bench_pipeline".to_string(), MockSourcePlan::new(actions))]);

                let recorder = MockSinkRecorder::default();
                let recorders = HashMap::from([("bench_pipeline".to_string(), recorder.clone())]);

                let builder = RuntimeBuilder::new()
                    .with_compiled_graph(compiled.clone())
                    .with_runtime_factory(caml::RuntimeAdapters::new(
                        Arc::new(caml::runtime::mock::MockSourceFactory::new(plans)),
                        Arc::new(caml::runtime::mock::MockSinkFactory::new(recorders)),
                    ));

                let handle = builder.start().await.unwrap();

                // Wait for EndOfStream
                loop {
                    if recorder.payloads().await.len() >= 20 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }

                handle.shutdown().await.unwrap();
            })
    });
}

criterion_group!(benches, bench_mock_passthrough);
criterion_main!(benches);
