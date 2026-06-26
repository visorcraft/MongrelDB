//! Page-encryption microbench: isolates AES-256-GCM-SIV encrypt/decrypt cost
//! from row materialization.
//!
//! Measures raw cipher throughput at page sizes that match the 65 536-row PAX
//! pages the engine writes, so the encryption overhead can be compared directly
//! against the decode/materialize cost measured by the other benches.
//!
//! Run: `cargo bench -p mongreldb-core --bench page_encryption --features encryption`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mongreldb_core::encryption::{AesCipher, Cipher};

fn bench_page_encryption(c: &mut Criterion) {
    let key = [0x42u8; 32];
    let cipher = AesCipher::new(&key).unwrap();
    let nonce = [0xABu8; 12];

    let sizes: &[(usize, &str)] = &[
        (4 * 1024, "4 KiB"),
        (64 * 1024, "64 KiB"),
        (256 * 1024, "256 KiB"),
        (1024 * 1024, "1 MiB"),
    ];

    let mut group = c.benchmark_group("page_encryption");
    group.measurement_time(std::time::Duration::from_secs(5));

    for &(size, label) in sizes {
        let page = vec![0x55u8; size];
        let ct = cipher.encrypt_page(&nonce, &page).unwrap();

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("encrypt", label), &page, |b, page| {
            b.iter(|| {
                let _ = black_box(cipher.encrypt_page(&nonce, page).unwrap());
            });
        });

        group.bench_with_input(BenchmarkId::new("decrypt", label), &ct, |b, ct| {
            b.iter(|| {
                let _ = black_box(cipher.decrypt_page(&nonce, ct).unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_page_encryption);
criterion_main!(benches);
