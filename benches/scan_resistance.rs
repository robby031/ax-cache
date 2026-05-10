use ax_cache::{Cache, MetricsSnapshot};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Zipf};
use std::time::{Duration, Instant};

fn print_header(title: &str) {
    println!();
    println!("=== {} ===", title);
}

fn fmt_pct(num: u64, denom: u64) -> String {
    if denom == 0 {
        "n/a".to_string()
    } else {
        format!("{:.2}%", 100.0 * num as f64 / denom as f64)
    }
}

const SR_CAP: usize = 10_000;
const SR_SHARDS: usize = 16;
const SR_HOT_SIZE: usize = 1_000;
const SR_SCAN_SIZE: usize = 100_000;
const SR_HOT_PRIME_PASSES: usize = 16;

fn run_scan() {
    let cache: Cache<u64, u64> = Cache::with_shards(SR_CAP, SR_SHARDS);

    for k in 0..SR_HOT_SIZE as u64 {
        cache.insert(k, k);
    }
    for _ in 0..SR_HOT_PRIME_PASSES {
        for k in 0..SR_HOT_SIZE as u64 {
            let _ = cache.get(&k);
        }
    }

    let m_before = cache.metrics();
    let scan_start = Instant::now();
    for i in 0..SR_SCAN_SIZE as u64 {
        let k = (SR_HOT_SIZE as u64) + i;
        cache.insert(k, k);
    }
    let scan_duration = scan_start.elapsed();

    let surviving: usize = (0..SR_HOT_SIZE as u64)
        .filter(|k| cache.get(k).is_some())
        .count();
    let m_after = cache.metrics();

    println!(
        "  hot_survivors={:>5}/{} ({:>5.1}%)  evicts_in_scan={:>6}  scan_wall={:>6.2}ms",
        surviving,
        SR_HOT_SIZE,
        100.0 * surviving as f64 / SR_HOT_SIZE as f64,
        m_after.evictions - m_before.evictions,
        scan_duration.as_secs_f64() * 1000.0,
    );
}

const CA_CAP: usize = 100_000;
const CA_SHARDS: usize = 16;
const CA_UNIVERSE: u64 = 200_000;
const CA_OPS: usize = 2_000_000;
const CA_ZIPF_ALPHA: f64 = 0.99;

fn run_cache_aside() {
    let cache: Cache<u64, u64> = Cache::with_shards(CA_CAP, CA_SHARDS);
    let zipf = Zipf::new(CA_UNIVERSE as f64, CA_ZIPF_ALPHA).expect("zipf");
    let mut rng = SmallRng::seed_from_u64(0xCAFE_BEEF_u64);

    let start = Instant::now();
    for _ in 0..CA_OPS {
        let k = zipf.sample(&mut rng) as u64;
        if cache.get(&k).is_none() {
            cache.insert(k, k);
        }
    }
    let duration = start.elapsed();
    let m = cache.metrics();
    print_workload_result(&m, duration);
}

const T3_CAP: usize = 10_000;
const T3_SHARDS: usize = 16;
const T3_HOT_KEYS: u64 = 1_000;
const T3_WARM_KEYS: u64 = 20_000;
const T3_COLD_KEYS: u64 = 100_000;
const T3_ACCESS_HOT_PCT: u64 = 60;
const T3_ACCESS_WARM_PCT: u64 = 35;
const T3_OPS: usize = 2_000_000;

fn run_3tier() {
    let cache: Cache<u64, u64> = Cache::with_shards(T3_CAP, T3_SHARDS);
    let mut rng = SmallRng::seed_from_u64(0xBADC_0FFE_u64);

    let start = Instant::now();
    for _ in 0..T3_OPS {
        let bucket = rng.random_range(0u64..100);
        let k = if bucket < T3_ACCESS_HOT_PCT {
            rng.random_range(0..T3_HOT_KEYS)
        } else if bucket < T3_ACCESS_HOT_PCT + T3_ACCESS_WARM_PCT {
            T3_HOT_KEYS + rng.random_range(0..T3_WARM_KEYS)
        } else {
            T3_HOT_KEYS + T3_WARM_KEYS + rng.random_range(0..T3_COLD_KEYS)
        };
        if cache.get(&k).is_none() {
            cache.insert(k, k);
        }
    }
    let duration = start.elapsed();
    let m = cache.metrics();
    print_workload_result(&m, duration);
}

fn print_workload_result(m: &MetricsSnapshot, duration: Duration) {
    let total = m.hits + m.misses;
    let throughput_mops = (total as f64) / duration.as_secs_f64() / 1e6;
    println!(
        "  hit_ratio={}  hits={:>9}  misses={:>9}  inserts={:>8}  evicts={:>8}  thr={:>5.2} Mops/s",
        fmt_pct(m.hits, total),
        m.hits,
        m.misses,
        m.insertions,
        m.evictions,
        throughput_mops,
    );
}

fn main() {
    print_header(&format!(
        "scenario 1: hot+scan  cap={} hot={} scan={}",
        SR_CAP, SR_HOT_SIZE, SR_SCAN_SIZE,
    ));
    run_scan();

    print_header(&format!(
        "scenario 2: cache-aside Zipf  cap={} universe={} alpha={} ops={}",
        CA_CAP, CA_UNIVERSE, CA_ZIPF_ALPHA, CA_OPS,
    ));
    run_cache_aside();

    print_header(&format!(
        "scenario 3: 3-tier  cap={} hot={} warm={}(>main_cap) cold={}  access=60/35/5  ops={}",
        T3_CAP, T3_HOT_KEYS, T3_WARM_KEYS, T3_COLD_KEYS, T3_OPS,
    ));
    run_3tier();
}
