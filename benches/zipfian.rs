use ax_cache::Cache;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Zipf};
use std::hint::black_box;

const CAPACITY: usize = 100_000;
const UNIVERSE: u64 = 1_000_000;
const SHARDS: usize = 16;
const WRITE_RATIO_PCT: u64 = 5;

fn make_cache_prewarmed() -> Cache<u64, u64> {
    let cache: Cache<u64, u64> = Cache::with_shards(CAPACITY, SHARDS);

    for i in 0..CAPACITY as u64 {
        cache.insert(i, i);
    }
    cache
}

fn bench_zipfian_mix(c: &mut Criterion) {
    let mut group = c.benchmark_group("zipfian_95_5");
    group.throughput(Throughput::Elements(1));

    for alpha in [0.7_f64, 0.99, 1.2] {
        group.bench_with_input(
            BenchmarkId::from_parameter(alpha),
            &alpha,
            |b, &alpha| {
                let cache = make_cache_prewarmed();
                let zipf = Zipf::new(UNIVERSE as f64, alpha).expect("valid zipf params");
                let mut rng = SmallRng::seed_from_u64(0xCAFE_BEEF_u64);
                let mut counter: u64 = 0;

                b.iter(|| {
                    let k = zipf.sample(&mut rng) as u64;
                    counter = counter.wrapping_add(1);
                    if counter % 100 < WRITE_RATIO_PCT {
                        cache.insert(black_box(k), black_box(counter));
                    } else {
                        let _ = black_box(cache.get(&k));
                    }
                });


                let m = cache.metrics();
                let total = m.hits + m.misses;
                if total > 0 {
                    eprintln!(
                        "  [zipf alpha={}] hits={} misses={} hit_ratio={:.4} insertions={} evictions={}",
                        alpha,
                        m.hits,
                        m.misses,
                        m.hits as f64 / total as f64,
                        m.insertions,
                        m.evictions,
                    );
                }
                let _ = &rng;
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_zipfian_mix);
criterion_main!(benches);
