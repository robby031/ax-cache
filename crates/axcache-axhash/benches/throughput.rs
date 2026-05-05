// crates/axcache-axhash/benches/throughput.rs

use axcache_axhash::RandomState;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;

fn bench_axhash_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("axhash_throughput");

    // Buat buffer 1 Megabyte (mewakili chunk besar)
    const SIZE: usize = 1024 * 1024;
    let data = vec![0u8; SIZE];
    let state = RandomState::new();

    // Set Criterion agar membagi hasil dengan jumlah byte (menghasilkan metrik kecepatan)
    group.throughput(Throughput::Bytes(SIZE as u64));

    group.bench_function("hash_1mb_chunk", |b| {
        b.iter(|| {
            let mut hasher = state.build_hasher();
            hasher.write(black_box(&data));
            black_box(hasher.finish());
        })
    });

    group.finish();
}

criterion_group!(benches, bench_axhash_throughput);
criterion_main!(benches);
