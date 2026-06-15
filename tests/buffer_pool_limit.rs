use caml::runtime::BufferPool;

#[test]
fn test_buffer_pool_preallocate_and_stats() {
    let pool = BufferPool::new(1024);
    assert_eq!(pool.stats().available, 0);
    assert_eq!(pool.stats().in_use, 0);
    assert_eq!(pool.stats().high_watermark, 0);

    pool.preallocate(10);
    assert_eq!(pool.stats().available, 10);
    assert_eq!(pool.stats().in_use, 0);
    assert_eq!(pool.stats().high_watermark, 10);

    let _buf1 = pool.acquire();
    assert_eq!(pool.stats().available, 9);
    assert_eq!(pool.stats().in_use, 1);
    assert_eq!(pool.stats().high_watermark, 10);
}

#[test]
fn test_buffer_pool_overflow_expansion() {
    let pool = BufferPool::new(1024);
    pool.preallocate(2);

    let _buf1 = pool.acquire();
    let _buf2 = pool.acquire();
    assert_eq!(pool.stats().available, 0);
    assert_eq!(pool.stats().in_use, 2);

    // Acquire another buffer, which should trigger dynamic allocation
    let _buf3 = pool.acquire();
    assert_eq!(pool.stats().available, 0);
    assert_eq!(pool.stats().in_use, 3);
    assert_eq!(pool.stats().high_watermark, 3);

    assert_eq!(pool.high_watermark_bytes(), 3 * 1024);
}
