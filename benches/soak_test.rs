use ax_cache::Cache;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};
use std::cell::RefCell;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_DURATION_SECS: u64 = 300;
const DEFAULT_CAPACITY: usize = 1_000_000;
const DEFAULT_UNIVERSE: u64 = 10_000_000;
const DEFAULT_SHARDS: usize = 64;
const DEFAULT_WRITE_RATIO: u64 = 5;
const DEFAULT_ALPHA: f64 = 0.99;
const DEFAULT_THREADS: usize = 8;
const DEFAULT_REPORT_INTERVAL: u64 = 5;
const DEFAULT_VALUE_SIZE: usize = 64;

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .unwrap_or(default)
}

#[derive(Clone)]
struct Config {
    duration_secs: u64,
    capacity: usize,
    universe: u64,
    shards: usize,
    write_ratio: u64,
    alpha: f64,
    threads: usize,
    report_interval_secs: u64,
    value_size: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: parse_env("SOAK_DURATION_SECS", DEFAULT_DURATION_SECS),
            capacity: parse_env("SOAK_CAPACITY", DEFAULT_CAPACITY),
            universe: parse_env("SOAK_UNIVERSE", DEFAULT_UNIVERSE),
            shards: parse_env("SOAK_SHARDS", DEFAULT_SHARDS),
            write_ratio: parse_env("SOAK_WRITE_RATIO", DEFAULT_WRITE_RATIO),
            alpha: parse_env("SOAK_ALPHA", DEFAULT_ALPHA),
            threads: parse_env("SOAK_THREADS", DEFAULT_THREADS),
            report_interval_secs: parse_env("SOAK_REPORT_INTERVAL_SECS", DEFAULT_REPORT_INTERVAL),
            value_size: parse_env("SOAK_VALUE_SIZE", DEFAULT_VALUE_SIZE),
        }
    }

    fn print(&self) {
        println!("=== ax-cache soak test ===");
        println!("duration_secs      = {}", self.duration_secs);
        println!("capacity           = {}", self.capacity);
        println!("universe           = {}", self.universe);
        println!("shards             = {}", self.shards);
        println!("write_ratio        = {}%", self.write_ratio);
        println!("zipf_alpha         = {:.2}", self.alpha);
        println!("threads            = {}", self.threads);
        println!("report_interval    = {}s", self.report_interval_secs);
        println!("value_size         = {} bytes", self.value_size);
        println!("===========================\n");
    }
}

#[derive(Default)]
struct WorkerCounters {
    ops: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
}

impl WorkerCounters {
    #[inline(always)]
    fn record_hit(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn record_miss(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn record_write(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
    }
}

struct WorkerResult {
    hist: Histogram<u64>,
}

thread_local! {
    static LOCAL_HIST: RefCell<Histogram<u64>> = RefCell::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("histogram")
    );
}

#[inline(always)]
fn record_latency(ns: u64) {
    LOCAL_HIST.with(|h| {
        let _ = h.borrow_mut().record(ns.max(1));
    });
}

fn snapshot_histogram() -> Histogram<u64> {
    LOCAL_HIST.with(|h| h.borrow().clone())
}

fn rss_mb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::fs;

        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let rss_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        let page_size = 4096u64;
        return Some((rss_pages * page_size) / 1024 / 1024);
    }

    #[cfg(target_os = "macos")]
    {
        use std::process::Command;

        let pid = std::process::id().to_string();
        let out = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .output()
            .ok()?;

        let rss_kb = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .ok()?;

        return Some(rss_kb / 1024);
    }

    #[allow(unreachable_code)]
    None
}

fn main() {
    let cfg = Config::from_env();
    cfg.print();

    type Value = Arc<[u8]>;

    println!("Initializing cache...");

    let cache: Arc<Cache<u64, Value>> = Arc::new(Cache::with_shards(cfg.capacity, cfg.shards));

    let payload: Value = vec![42u8; cfg.value_size].into();

    println!("Prewarming cache...");
    for i in 0..cfg.capacity as u64 {
        cache.insert(i, Arc::clone(&payload));
    }

    println!("Prewarm done.\n");

    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(cfg.threads);
    let mut counters = Vec::with_capacity(cfg.threads);

    for tid in 0..cfg.threads {
        let cache = Arc::clone(&cache);
        let stop = Arc::clone(&stop);
        let start_signal = Arc::clone(&start);
        let payload = Arc::clone(&payload);

        let counter = Arc::new(WorkerCounters::default());
        counters.push(Arc::clone(&counter));

        let cfg = cfg.clone();

        handles.push(thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(0xC0FFEE_u64 ^ tid as u64);
            let zipf = Zipf::new(cfg.universe as f64, cfg.alpha).expect("zipf");

            while !start_signal.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            while !stop.load(Ordering::Acquire) {
                let key = zipf.sample(&mut rng) as u64;
                let is_write = rng.random_range(0..100u64) < cfg.write_ratio;

                let t0 = Instant::now();

                if is_write {
                    cache.insert(key, Arc::clone(&payload));
                    counter.record_write();
                } else if cache.get(&key).is_some() {
                    counter.record_hit();
                } else {
                    counter.record_miss();
                }

                let latency_ns = t0.elapsed().as_nanos() as u64;
                record_latency(latency_ns);
            }

            WorkerResult {
                hist: snapshot_histogram(),
            }
        }));
    }

    println!("Starting workers...\n");

    start.store(true, Ordering::Release);

    let bench_start = Instant::now();
    let total_duration = Duration::from_secs(cfg.duration_secs);
    let report_interval = Duration::from_secs(cfg.report_interval_secs);

    let mut prev_ops = 0u64;
    let mut prev_hits = 0u64;
    let mut prev_misses = 0u64;
    let mut prev_writes = 0u64;

    let mut peak_rss_mb = 0u64;

    while bench_start.elapsed() < total_duration {
        thread::sleep(report_interval);

        let mut total_ops = 0u64;
        let mut total_hits = 0u64;
        let mut total_misses = 0u64;
        let mut total_writes = 0u64;

        for c in &counters {
            total_ops += c.ops.load(Ordering::Relaxed);
            total_hits += c.hits.load(Ordering::Relaxed);
            total_misses += c.misses.load(Ordering::Relaxed);
            total_writes += c.writes.load(Ordering::Relaxed);
        }

        let ops_delta = total_ops - prev_ops;
        let hits_delta = total_hits - prev_hits;
        let misses_delta = total_misses - prev_misses;
        let writes_delta = total_writes - prev_writes;

        prev_ops = total_ops;
        prev_hits = total_hits;
        prev_misses = total_misses;
        prev_writes = total_writes;

        let throughput = ops_delta as f64 / cfg.report_interval_secs as f64 / 1e6;

        let total_reads = hits_delta + misses_delta;

        let hit_ratio = if total_reads > 0 {
            hits_delta as f64 / total_reads as f64
        } else {
            0.0
        };

        let metrics = cache.metrics();

        let rss = rss_mb().unwrap_or(0);
        peak_rss_mb = peak_rss_mb.max(rss);

        println!(
            "[{elapsed:>5}s] thr={thr:>7.2} Mops/s | hit={hit_ratio:.4} | reads={reads:>10} | writes={writes:>9} | entries={entries:>8} | evict={evict:>9} | reject={reject:>9} | rss={rss:>5} MB",
            elapsed = bench_start.elapsed().as_secs(),
            thr = throughput,
            reads = total_reads,
            writes = writes_delta,
            entries = cache.len(),
            evict = metrics.evictions,
            reject = metrics.rejections,
            rss = rss,
        );
    }

    println!("\nStopping workers...");

    stop.store(true, Ordering::Release);

    let mut merged_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram");

    for h in handles {
        let res = h.join().expect("worker join");
        merged_hist.add(&res.hist).expect("merge hist");
    }

    let elapsed = bench_start.elapsed();

    let mut final_ops = 0u64;
    let mut final_hits = 0u64;
    let mut final_misses = 0u64;
    let mut final_writes = 0u64;

    for c in &counters {
        final_ops += c.ops.load(Ordering::Relaxed);
        final_hits += c.hits.load(Ordering::Relaxed);
        final_misses += c.misses.load(Ordering::Relaxed);
        final_writes += c.writes.load(Ordering::Relaxed);
    }

    let throughput = final_ops as f64 / elapsed.as_secs_f64() / 1e6;

    let hit_ratio = if final_hits + final_misses > 0 {
        final_hits as f64 / (final_hits + final_misses) as f64
    } else {
        0.0
    };

    println!("\n=== FINAL REPORT ===\n");

    println!("elapsed_secs      : {:.2}", elapsed.as_secs_f64());
    println!("total_ops         : {}", final_ops);
    println!("throughput        : {:.2} Mops/s", throughput);
    println!("hit_ratio         : {:.4}", hit_ratio);
    println!("total_hits        : {}", final_hits);
    println!("total_misses      : {}", final_misses);
    println!("total_writes      : {}", final_writes);
    println!("peak_rss_mb       : {}", peak_rss_mb);

    println!("\nLatency Percentiles:");
    println!("P50   = {:>8} ns", merged_hist.value_at_quantile(0.50));
    println!("P90   = {:>8} ns", merged_hist.value_at_quantile(0.90));
    println!("P95   = {:>8} ns", merged_hist.value_at_quantile(0.95));
    println!("P99   = {:>8} ns", merged_hist.value_at_quantile(0.99));
    println!("P999  = {:>8} ns", merged_hist.value_at_quantile(0.999));
    println!("MAX   = {:>8} ns", merged_hist.max());

    let metrics = cache.metrics();

    println!("\nCache Metrics:");
    println!("hits        = {}", metrics.hits);
    println!("misses      = {}", metrics.misses);
    println!("insertions  = {}", metrics.insertions);
    println!("evictions   = {}", metrics.evictions);
    println!("rejections  = {}", metrics.rejections);
    println!("entries     = {}", cache.len());

    println!("\n=== DONE ===");
}
