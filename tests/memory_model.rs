use caml::runtime::{BufferPool, EncodedPacket, MediaStorage};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_buffer_pool_preallocate_and_stats() {
    let pool = BufferPool::new(1024);
    let stats = pool.stats();
    assert_eq!(stats.available, 0);
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.high_watermark, 0);

    pool.preallocate(5);
    let stats = pool.stats();
    assert_eq!(stats.available, 5);
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.high_watermark, 5);

    let buf1 = pool.acquire();
    let stats = pool.stats();
    assert_eq!(stats.available, 4);
    assert_eq!(stats.in_use, 1);
    assert_eq!(stats.high_watermark, 5);

    let buf2 = pool.acquire();
    let stats = pool.stats();
    assert_eq!(stats.available, 3);
    assert_eq!(stats.in_use, 2);

    drop(buf1);
    let stats = pool.stats();
    assert_eq!(stats.available, 4);
    assert_eq!(stats.in_use, 1);

    drop(buf2);
    let stats = pool.stats();
    assert_eq!(stats.available, 5);
    assert_eq!(stats.in_use, 0);
}

#[test]
fn test_buffer_pool_dynamic_allocation() {
    let pool = BufferPool::new(1024);

    // Acquire when pool is empty should allocate dynamically
    let buf1 = pool.acquire();
    let stats = pool.stats();
    assert_eq!(stats.available, 0);
    assert_eq!(stats.in_use, 1);
    assert_eq!(stats.high_watermark, 1);

    drop(buf1);
    let stats = pool.stats();
    assert_eq!(stats.available, 1);
    assert_eq!(stats.in_use, 0);
}

#[test]
fn test_media_storage_variants() {
    let pool = BufferPool::new(1024);
    let mut buf = pool.acquire();
    buf.extend_from_slice(&[1, 2, 3]);

    let storage_pooled = MediaStorage::Pooled(buf.freeze());
    assert_eq!(storage_pooled.as_slice(), &[1, 2, 3]);
    assert_eq!(storage_pooled.len(), 3);
    assert!(!storage_pooled.is_empty());

    let storage_owned = MediaStorage::Owned(Arc::new(vec![4, 5, 6]));
    assert_eq!(storage_owned.as_slice(), &[4, 5, 6]);
    assert_eq!(storage_owned.len(), 3);
}

#[test]
fn test_encoded_packet_with_media_storage() {
    let pool = BufferPool::new(1024);
    let mut buf = pool.acquire();
    buf.extend_from_slice(&[7, 8, 9]);

    let packet = EncodedPacket {
        codec: "h264".to_string(),
        timestamp: Some(Duration::from_millis(100)),
        duration: Some(Duration::from_millis(33)),
        is_keyframe: true,
        data: MediaStorage::Pooled(buf.freeze()),
    };

    assert_eq!(packet.data.as_slice(), &[7, 8, 9]);
}
