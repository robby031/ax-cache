// Run with:
// cargo bench --bench single_thread

use ax_cache::Cache;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const CAPACITY: usize = 100_000;
const SHARDS: usize = 16;

fn populate(cache: &Cache<u64, u64>) {
    for i in 0..CAPACITY as u64 {
        cache.insert(i, i);
    }
}

fn bench_get_hit(c: &mut Criterion) {
    let cache: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate(&cache);

    let mut group = c.benchmark_group("single_thread");
    group.throughput(Throughput::Elements(1));
    group.bench_function("get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let v = cache.get(black_box(&(i % CAPACITY as u64)));
            i = i.wrapping_add(1);
            v
        })
    });
    group.finish();
}

fn bench_get_miss(c: &mut Criterion) {
    let cache: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate(&cache);

    let mut group = c.benchmark_group("single_thread");
    group.throughput(Throughput::Elements(1));
    group.bench_function("get_miss", |b| {
        let mut i = (CAPACITY * 10) as u64;
        b.iter(|| {
            let v = cache.get(black_box(&i));
            i = i.wrapping_add(1);
            v
        })
    });
    group.finish();
}

fn bench_insert_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread");
    group.throughput(Throughput::Elements(1));
    group.bench_function("insert_new", |b| {
        // Capacity well above the iteration count so we measure pure insert
        // cost without eviction noise. (Eviction is exercised by zipfian.)
        let cache: Cache<u64, u64> = Cache::with_shards(10_000_000, SHARDS);
        let mut i = 0u64;
        b.iter(|| {
            cache.insert(black_box(i), black_box(i));
            i = i.wrapping_add(1);
        })
    });
    group.finish();
}

fn bench_insert_update(c: &mut Criterion) {
    let cache: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate(&cache);

    let mut group = c.benchmark_group("single_thread");
    group.throughput(Throughput::Elements(1));
    group.bench_function("insert_update", |b| {
        let mut i = 0u64;
        b.iter(|| {
            cache.insert(black_box(i % CAPACITY as u64), black_box(i));
            i = i.wrapping_add(1);
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_get_hit,
    bench_get_miss,
    bench_insert_new,
    bench_insert_update
);
criterion_main!(benches);
