use ax_cache::Cache;
use hdrhistogram::Histogram;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use rand_distr::{Distribution, Zipf};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_CAPACITY: usize = 1_000_000;
const DEFAULT_UNIVERSE: u64 = 10_000_000;
const DEFAULT_SHARDS: usize = 64;
const DEFAULT_WRITE_RATIO_PCT: u64 = 5;
const DEFAULT_ZIPF_ALPHA: f64 = 0.99;

fn parse_threads() -> Vec<usize> {
    match env::var("AXCACHE_BENCH_THREADS") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse().ok())
            .filter(|n: &usize| *n > 0)
            .collect(),
        Err(_) => vec![1, 2, 4, 8, 16],
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_ops_per_thread() -> usize {
    parse_env("AXCACHE_BENCH_OPS", 500_000)
}

struct Config {
    capacity: usize,
    universe: u64,
    shards: usize,
    write_pct: u64,
    zipf_alpha: f64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            capacity: parse_env("AXCACHE_BENCH_CAP", DEFAULT_CAPACITY),
            universe: parse_env("AXCACHE_BENCH_UNIVERSE", DEFAULT_UNIVERSE),
            shards: parse_env("AXCACHE_BENCH_SHARDS", DEFAULT_SHARDS),
            write_pct: parse_env("AXCACHE_BENCH_WRITE_PCT", DEFAULT_WRITE_RATIO_PCT),
            zipf_alpha: parse_env("AXCACHE_BENCH_ALPHA", DEFAULT_ZIPF_ALPHA),
        }
    }
}

fn run_sweep(cfg: &Config, threads: usize, ops_per_thread: usize) -> SweepResult {
    let cache: Arc<Cache<u64, u64>> = Arc::new(Cache::with_shards(cfg.capacity, cfg.shards));
    for i in 0..cfg.capacity as u64 {
        cache.insert(i, i);
    }

    let go = Arc::new(AtomicBool::new(false));
    let universe = cfg.universe;
    let alpha = cfg.zipf_alpha;
    let write_pct = cfg.write_pct;
    let mut handles = Vec::with_capacity(threads);
    for tid in 0..threads {
        let cache = Arc::clone(&cache);
        let go = Arc::clone(&go);
        handles.push(thread::spawn(move || {
            let mut hist = Histogram::<u64>::new(3).expect("histogram");
            let mut rng = SmallRng::seed_from_u64(0xC0FFEE ^ tid as u64);
            let zipf = Zipf::new(universe as f64, alpha).expect("zipf");

            while !go.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            let started = Instant::now();
            for _ in 0..ops_per_thread {
                let k = zipf.sample(&mut rng) as u64;
                let is_write = write_pct > 0 && rng.random_range(0u64..100) < write_pct;

                let t0 = Instant::now();
                if is_write {
                    cache.insert(k, k);
                } else {
                    let _ = cache.get(&k);
                }
                let ns = t0.elapsed().as_nanos() as u64;
                let _ = hist.record(ns.max(1));
            }
            let elapsed = started.elapsed();
            (hist, elapsed)
        }));
    }

    go.store(true, Ordering::Release);

    let mut combined = Histogram::<u64>::new(3).expect("histogram");
    let mut wall = Duration::ZERO;
    for h in handles {
        let (hist, elapsed) = h.join().expect("worker join");
        combined.add(&hist).expect("histogram merge");
        wall = wall.max(elapsed);
    }

    let metrics = cache.metrics();
    SweepResult {
        threads,
        ops_per_thread,
        wall,
        hist: combined,
        hit_ratio: {
            let total = metrics.hits + metrics.misses;
            if total == 0 {
                0.0
            } else {
                metrics.hits as f64 / total as f64
            }
        },
        evictions: metrics.evictions,
    }
}

struct SweepResult {
    threads: usize,
    ops_per_thread: usize,
    wall: Duration,
    hist: Histogram<u64>,
    hit_ratio: f64,
    evictions: u64,
}

impl SweepResult {
    fn print(&self) {
        let total_ops = self.threads as u64 * self.ops_per_thread as u64;
        let throughput_mops = (total_ops as f64) / self.wall.as_secs_f64() / 1e6;
        println!(
            "threads={:>3}  ops/thread={:>9}  wall={:>7.2}ms  thr={:>7.2} Mops/s  \
             p50={:>4}ns  p90={:>5}ns  p99={:>6}ns  p999={:>7}ns  max={:>8}ns  \
             hit_ratio={:.4}  evictions={}",
            self.threads,
            self.ops_per_thread,
            self.wall.as_secs_f64() * 1000.0,
            throughput_mops,
            self.hist.value_at_quantile(0.50),
            self.hist.value_at_quantile(0.90),
            self.hist.value_at_quantile(0.99),
            self.hist.value_at_quantile(0.999),
            self.hist.max(),
            self.hit_ratio,
            self.evictions,
        );
    }
}

fn main() {
    let cfg = Config::from_env();
    let threads_list = parse_threads();
    let ops = parse_ops_per_thread();
    println!(
        "ax-cache contention sweep — capacity={} universe={} shards={} write%={} zipf_alpha={}",
        cfg.capacity, cfg.universe, cfg.shards, cfg.write_pct, cfg.zipf_alpha,
    );
    println!("ops_per_thread={}  threads={:?}", ops, threads_list,);
    println!();
    for &t in &threads_list {
        let r = run_sweep(&cfg, t, ops);
        r.print();
    }
}
