use ax_cache::Cache;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use moka::sync::Cache as MokaCache;
use std::hint::black_box;

const CAPACITY: usize = 100_000;
const SHARDS: usize = 16;

fn populate_ax(cache: &Cache<u64, u64>) {
    for i in 0..CAPACITY as u64 {
        cache.insert(i, i);
    }
}

fn populate_moka(cache: &MokaCache<u64, u64>) {
    for i in 0..CAPACITY as u64 {
        cache.insert(i, i);
    }
}

fn bench_get_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("ax_vs_moka_get_hit");
    group.throughput(Throughput::Elements(1));

    // ax-cache
    let cache_ax: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate_ax(&cache_ax);
    group.bench_function("ax_cache", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let v = cache_ax.get(black_box(&(i % CAPACITY as u64)));
            i = i.wrapping_add(1);
            v
        })
    });

    // moka
    let cache_moka: MokaCache<u64, u64> = MokaCache::new(CAPACITY as u64);
    populate_moka(&cache_moka);
    group.bench_function("moka", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let v = cache_moka.get(black_box(&(i % CAPACITY as u64)));
            i = i.wrapping_add(1);
            v
        })
    });

    group.finish();
}

fn bench_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("ax_vs_moka_get_miss");
    group.throughput(Throughput::Elements(1));

    // ax-cache
    let cache_ax: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate_ax(&cache_ax);
    group.bench_function("ax_cache", |b| {
        let mut i = (CAPACITY * 10) as u64;
        b.iter(|| {
            let v = cache_ax.get(black_box(&i));
            i = i.wrapping_add(1);
            v
        })
    });

    // moka
    let cache_moka: MokaCache<u64, u64> = MokaCache::new(CAPACITY as u64);
    populate_moka(&cache_moka);
    group.bench_function("moka", |b| {
        let mut i = (CAPACITY * 10) as u64;
        b.iter(|| {
            let v = cache_moka.get(black_box(&i));
            i = i.wrapping_add(1);
            v
        })
    });

    group.finish();
}

fn bench_insert_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("ax_vs_moka_insert_new");
    group.throughput(Throughput::Elements(1));

    // ax-cache
    group.bench_function("ax_cache", |b| {
        let cache_ax: Cache<u64, u64> = Cache::with_shards(10_000_000, SHARDS);
        let mut i = 0u64;
        b.iter(|| {
            cache_ax.insert(black_box(i), black_box(i));
            i = i.wrapping_add(1);
        })
    });

    // moka
    group.bench_function("moka", |b| {
        let cache_moka: MokaCache<u64, u64> = MokaCache::new(10_000_000_u64);
        let mut i = 0u64;
        b.iter(|| {
            cache_moka.insert(black_box(i), black_box(i));
            i = i.wrapping_add(1);
        })
    });

    group.finish();
}

fn bench_insert_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("ax_vs_moka_insert_update");
    group.throughput(Throughput::Elements(1));

    // ax-cache
    let cache_ax: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);
    populate_ax(&cache_ax);
    group.bench_function("ax_cache", |b| {
        let mut i = 0u64;
        b.iter(|| {
            cache_ax.insert(black_box(i % CAPACITY as u64), black_box(i));
            i = i.wrapping_add(1);
        })
    });

    // moka
    let cache_moka: MokaCache<u64, u64> = MokaCache::new(CAPACITY as u64);
    populate_moka(&cache_moka);
    group.bench_function("moka", |b| {
        let mut i = 0u64;
        b.iter(|| {
            cache_moka.insert(black_box(i % CAPACITY as u64), black_box(i));
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
