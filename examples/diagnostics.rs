// Diagnostic harness for ax-cache.
//
// What this IS:
//   A read-out of how the cache behaves under specific scenarios, using
//   ONLY the public API and exposed metrics. All RNG is seeded so the
//   workload is reproducible; only wall-clock timings vary run-to-run.
//
// What this is NOT:
//   - A performance benchmark (use criterion benches for that).
//   - A correctness test (use `cargo test` for that).
//   - A capacity-planning tool.
//
// Honesty notes:
//   - Throughput numbers depend on machine, load, and thermals. Reported
//     numbers are one observation, not a steady-state estimate.
//   - We do NOT measure per-shard load balance — the public API does not
//     expose per-shard counts, so we measure throughput scaling instead
//     (an indirect signal of sharding's effect on lock contention).
//   - TTL "alive" counts reflect what get() returns, not memory footprint.
//     ax-cache TTL is lazy; expired entries persist in storage until they
//     are accessed or swept by maintenance.
//   - Memory reclamation is observed via len() after symmetric churn. We
//     do NOT report RSS; that's OS-dependent and dominated by allocator
//     behavior, which would mislead more than inform.

use ax_cache::{Cache, MaintenanceConfig};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn header(s: &str) {
    println!();
    println!("=== {} ===", s);
}

fn note(s: &str) {
    println!("  note: {}", s);
}

fn machine_info() {
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    println!("machine: reported_parallelism={}", cores);
    println!("build:   profile=release (run via `cargo run --release --example diagnostics`)");
}

// -------------------------------------------------------------------------
// 1. eviction
// -------------------------------------------------------------------------
fn diag_eviction() {
    header("eviction: insert 2x capacity, measure final len + eviction count");
    const CAP: usize = 10_000;
    const SHARDS: usize = 16;
    const INSERTS: u64 = (CAP as u64) * 2;

    let c: Cache<u64, u64> = Cache::with_shards(CAP, SHARDS);
    let t0 = Instant::now();
    for i in 0..INSERTS {
        c.insert(i, i);
    }
    let dur = t0.elapsed();
    let m = c.metrics();

    println!(
        "  cap={CAP}  shards={SHARDS}  inserted={INSERTS}  \
         final_len={}  evictions={}  rejections={}  wall={:.1}ms",
        c.len(),
        m.evictions,
        m.rejections,
        dur.as_secs_f64() * 1000.0,
    );
    note("S3-FIFO + TinyLFU admission is approximate. final_len is expected to be near CAP but may overshoot transiently.");
    note("rejections = items dropped at the door by the admission filter (cold first-touch under capacity pressure).");
}

// -------------------------------------------------------------------------
// 2. sharding
// -------------------------------------------------------------------------
fn diag_sharding() {
    header("sharding: write throughput vs shard count under 8-thread contention");
    const THREADS: usize = 8;
    const PER_THREAD: u64 = 200_000;
    const CAP: usize = 1_000_000;

    println!("  threads={THREADS}  per_thread_ops={PER_THREAD}  cap={CAP}");
    for shards in [1usize, 2, 4, 8, 16, 32, 64] {
        let c = Arc::new(Cache::<u64, u64>::with_shards(CAP, shards));
        let t0 = Instant::now();
        let handles: Vec<_> = (0..THREADS as u64)
            .map(|t| {
                let c = Arc::clone(&c);
                thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let k = t * 10_000_000 + i;
                        c.insert(k, k);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let dur = t0.elapsed();
        let total_ops = PER_THREAD * THREADS as u64;
        let mops = (total_ops as f64) / dur.as_secs_f64() / 1e6;
        println!(
            "  shards={shards:>3}  total_ops={total_ops:>9}  wall={:>6.1}ms  thr={:>5.2} Mops/s",
            dur.as_secs_f64() * 1000.0,
            mops,
        );
    }
    note("higher Mops/s with more shards = sharding reduces write-lock contention.");
    note("plateau usually means bottleneck moved off the per-shard lock (allocator, memory bandwidth, NUMA).");
    note("we do NOT measure per-shard key distribution here; that would require non-public instrumentation.");
}

// -------------------------------------------------------------------------
// 3. TTL
// -------------------------------------------------------------------------
fn diag_ttl() {
    header("TTL: 1000 entries with TTL=200ms, alive count over time");
    const N: u64 = 1000;
    const TTL_MS: u64 = 200;

    let c: Cache<u64, u64> = Cache::with_shards((N as usize) * 2, 8);
    let t_insert = Instant::now();
    for k in 0..N {
        c.insert_with_ttl(k, k, Duration::from_millis(TTL_MS));
    }
    println!("  inserted={N}  ttl_ms={TTL_MS}  (no maintenance thread — purely lazy expiry via get())");

    for ms in [0u64, 50, 100, 150, 195, 205, 250, 400] {
        let target = Duration::from_millis(ms);
        let now = t_insert.elapsed();
        if target > now {
            thread::sleep(target - now);
        }
        let alive = (0..N).filter(|k| c.get(k).is_some()).count();
        println!("  t+{ms:>4}ms  alive_via_get={alive:>4}/{N}");
    }
    note("expected: alive ~= N until t < TTL, then drops to 0 once past TTL. Sampling itself takes time so boundary samples can drift a few ms.");
    note("ax-cache TTL is lazy: storage may still hold expired entries between samples; we only report what get() observes.");
}

// -------------------------------------------------------------------------
// 4. concurrent contention
// -------------------------------------------------------------------------
fn diag_contention() {
    header("concurrent contention: get-or-insert on small shared universe, vs thread count");
    const OPS_PER_THREAD: usize = 500_000;
    const UNIVERSE: u64 = 20_000;
    const CAP: usize = 100_000;
    const SHARDS: usize = 16;

    println!(
        "  per_thread_ops={OPS_PER_THREAD}  universe={UNIVERSE}  cap={CAP}  shards={SHARDS}"
    );
    for threads in [1usize, 2, 4, 8, 16] {
        let c = Arc::new(Cache::<u64, u64>::with_shards(CAP, SHARDS));
        let t0 = Instant::now();
        let handles: Vec<_> = (0..threads as u64)
            .map(|t| {
                let c = Arc::clone(&c);
                thread::spawn(move || {
                    let mut rng = SmallRng::seed_from_u64(0xA1B2_C3D4 ^ t);
                    for _ in 0..OPS_PER_THREAD {
                        let k = rng.random_range(0u64..UNIVERSE);
                        if c.get(&k).is_none() {
                            c.insert(k, k);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let dur = t0.elapsed();
        let total = (OPS_PER_THREAD * threads) as f64;
        let agg = total / dur.as_secs_f64() / 1e6;
        let per = agg / threads as f64;
        println!(
            "  threads={threads:>2}  total_ops={:>9}  wall={:>6.1}ms  agg={:>5.2} Mops/s  per_thread={:>5.2} Mops/s",
            OPS_PER_THREAD * threads,
            dur.as_secs_f64() * 1000.0,
            agg,
            per,
        );
    }
    note("ideal scaling: agg grows linearly with threads, per_thread stays flat. real systems flatten as cores oversubscribe shards/cache lines.");
}

// -------------------------------------------------------------------------
// 5. memory reclamation
// -------------------------------------------------------------------------
fn diag_memory_reclamation() {
    header("memory reclamation: symmetric insert+remove churn, final len check");
    const CAP: usize = 10_000;
    const CYCLES: usize = 100;
    const PER_CYCLE: u64 = 5_000;

    let c: Cache<u64, u64> = Cache::with_shards(CAP, 4);
    let t0 = Instant::now();
    for cycle in 0..CYCLES as u64 {
        let base = cycle * 1_000_000;
        for i in 0..PER_CYCLE {
            c.insert(base + i, base + i);
        }
        for i in 0..PER_CYCLE {
            c.remove(&(base + i));
        }
    }
    let dur = t0.elapsed();
    println!(
        "  cap={CAP}  cycles={CYCLES}  per_cycle_ins+del={}  final_len={}  wall={:.1}ms",
        PER_CYCLE * 2,
        c.len(),
        dur.as_secs_f64() * 1000.0,
    );
    note("expected: final_len == 0. nonzero would indicate eviction-queue or hashmap entries that survived their key being removed.");
    note("this does NOT report process RSS — allocator behavior would dominate, masking the signal we actually care about.");
}

// -------------------------------------------------------------------------
// 6. fairness
// -------------------------------------------------------------------------
fn diag_fairness() {
    header("fairness: per-thread completion time under contention");
    const THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 250_000;
    const UNIVERSE: u64 = 10_000;
    const CAP: usize = 50_000;
    const SHARDS: usize = 16;

    let c = Arc::new(Cache::<u64, u64>::with_shards(CAP, SHARDS));
    let handles: Vec<_> = (0..THREADS as u64)
        .map(|t| {
            let c = Arc::clone(&c);
            thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ t);
                let t0 = Instant::now();
                for _ in 0..OPS_PER_THREAD {
                    let k = rng.random_range(0u64..UNIVERSE);
                    if c.get(&k).is_none() {
                        c.insert(k, k);
                    }
                }
                t0.elapsed().as_secs_f64() * 1000.0
            })
        })
        .collect();
    let mut times: Vec<f64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let min = times[0];
    let med = times[times.len() / 2];
    let max = *times.last().unwrap();
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let variance =
        times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times.len() as f64;
    let cv = variance.sqrt() / mean;

    println!(
        "  threads={THREADS}  per_thread_ops={OPS_PER_THREAD}  cap={CAP}  shards={SHARDS}"
    );
    println!(
        "  per-thread wall (ms): min={min:.1}  median={med:.1}  max={max:.1}  mean={mean:.1}  CV={cv:.3}"
    );
    note("CV (coefficient of variation = stddev/mean) close to 0 = threads finish in lockstep. higher CV = some threads waited longer (lock starvation, scheduling, or skewed hash routing).");
    note("a small CV (~0.05) is normal jitter; CVs > 0.20 in a uniform workload would be worth investigating.");
}

// -------------------------------------------------------------------------
// 7. stale entry handling
// -------------------------------------------------------------------------
fn diag_stale() {
    header("stale entry handling: maintenance thread sweep of expired entries");
    const N: u32 = 500;
    const TTL_MS: u64 = 50;
    const SWEEP_MS: u64 = 25;
    const BUDGET: usize = 64;

    let c: Cache<u32, u32> = Cache::with_shards(1024, 4);
    c.enable_maintenance(MaintenanceConfig {
        sweep_interval: Duration::from_millis(SWEEP_MS),
        max_sweep_per_shard: BUDGET,
    });
    let t0 = Instant::now();
    for k in 0..N {
        c.insert_with_ttl(k, k, Duration::from_millis(TTL_MS));
    }
    println!(
        "  inserted={N}  ttl_ms={TTL_MS}  sweep_interval_ms={SWEEP_MS}  budget_per_shard={BUDGET}  shards={}",
        c.shard_count()
    );

    for ms in [0u64, 25, 50, 100, 200, 400, 800] {
        let target = Duration::from_millis(ms);
        let now = t0.elapsed();
        if target > now {
            thread::sleep(target - now);
        }
        println!("  t+{ms:>4}ms  len={:>4}", c.len());
    }
    note("expected: len shrinks from ~N toward 0 once past TTL, with rate bounded by SWEEP_MS and budget*shards per tick.");
    note("len() reflects storage occupancy, NOT what get() would still serve. for the get()-perspective see the TTL section.");
}

fn main() {
    machine_info();
    diag_eviction();
    diag_sharding();
    diag_ttl();
    diag_contention();
    diag_memory_reclamation();
    diag_fairness();
    diag_stale();
    println!();
    println!("=== diagnostics complete ===");
}
