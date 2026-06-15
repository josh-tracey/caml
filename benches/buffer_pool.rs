use criterion::{criterion_group, criterion_main, Criterion};
use caml::runtime::BufferPool;

fn bench_buffer_pool_acquire_release(c: &mut Criterion) {
    let pool = BufferPool::new(1024);
    
    // Preallocate to avoid allocations during measurement
    pool.preallocate(100);

    c.bench_function("buffer_pool_acquire_release", |b| {
        b.iter(|| {
            let buf = pool.acquire();
            // Drop buffer to return it to the pool
            let _ = buf;
        })
    });
}

criterion_group!(benches, bench_buffer_pool_acquire_release);
criterion_main!(benches);
