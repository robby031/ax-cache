# ax-cache

A concurrent in-memory cache for Rust with admission-controlled eviction.

[![Crates.io](https://img.shields.io/crates/v/ax-cache.svg)](https://crates.io/crates/ax-cache)
[![Docs.rs](https://docs.rs/ax-cache/badge.svg)](https://docs.rs/ax-cache)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What it is

- **Sharded** [`hashbrown`] map with one [`parking_lot::RwLock`] per shard.
- **S3-FIFO eviction** (small-queue / main-queue split with frequency-based promotion).
- **TinyLFU admission** (Count-Min Sketch) keeps one-hit-wonders from displacing established hot keys.
- **AES-NI accelerated hashing** via [`axhash`] when the CPU supports it; portable fallback otherwise.

[`hashbrown`]: https://crates.io/crates/hashbrown
[`parking_lot::RwLock`]: https://crates.io/crates/parking_lot
[`axhash`]: https://crates.io/crates/axhash-core

---

## Install

```toml
[dependencies]
ax-cache = "0.1"
```

No feature flags. AES detection is automatic at runtime.

---

## API at a glance

```rust
use ax_cache::{Cache, InsertOutcome};
use std::time::Duration;

// Construction
let cache: Cache<String, u64> = Cache::new(10_000);            // auto-shard
let cache: Cache<String, u64> = Cache::with_shards(10_000, 16); // explicit

// Reads — accept any borrowed form (e.g. `&str` for `String` keys, no allocation).
let value: Option<u64>    = cache.get("alpha");
let exists: bool          = cache.contains_key("alpha");
let removed: Option<u64>  = cache.remove("alpha");

// Writes — return InsertOutcome (Inserted / Updated / Rejected).
let r: InsertOutcome = cache.insert("alpha".to_string(), 1);
let r: InsertOutcome = cache.insert_with_ttl("session".to_string(), 1, Duration::from_secs(60));
assert!(r.is_present()); // == Inserted | Updated

// Bulk
cache.clear();
let n: usize = cache.len();
let empty: bool = cache.is_empty();

// Observability
let m = cache.metrics(); // hits, misses, insertions, evictions, rejections
```

`InsertOutcome` makes write semantics explicit:

| Variant     | Meaning                                                           | `is_present()` |
|-------------|-------------------------------------------------------------------|:--------------:|
| `Inserted`  | Key was new and admitted into the cache.                          | `true`         |
| `Updated`   | Key already existed; value (and TTL) was replaced.                | `true`         |
| `Rejected`  | Cache was full; admission filter declined this key.               | `false`        |

---

## Examples

### Basic

```rust
use ax_cache::Cache;

let cache: Cache<String, Vec<u8>> = Cache::new(10_000);

cache.insert("user:1001".to_string(), vec![1, 2, 3]);

if let Some(data) = cache.get("user:1001") {
    println!("found: {} bytes", data.len());
}
```

### TTL

```rust
use ax_cache::Cache;
use std::time::Duration;

let cache: Cache<String, String> = Cache::new(50_000);
cache.insert_with_ttl(
    "session:abc".to_string(),
    "token".to_string(),
    Duration::from_secs(30),
);

assert!(cache.get("session:abc").is_some());
```

Expired entries are reclaimed lazily on access. To reclaim them
proactively in the background, opt into the maintenance thread:

```rust
use ax_cache::{Cache, MaintenanceConfig};
use std::time::Duration;

let cache: Cache<u64, Vec<u8>> = Cache::new(100_000);
cache.enable_maintenance(MaintenanceConfig {
    sweep_interval: Duration::from_millis(250),
    max_sweep_per_shard: 128,
});
```

The maintenance thread stops automatically when the cache is dropped.

### Shared across threads

```rust
use ax_cache::Cache;
use std::sync::Arc;
use std::thread;

let cache = Arc::new(Cache::<u64, u64>::new(100_000));

let handles: Vec<_> = (0..8u64).map(|t| {
    let c = Arc::clone(&cache);
    thread::spawn(move || {
        for i in 0..10_000u64 {
            c.insert(t * 100_000 + i, i);
        }
    })
}).collect();

for h in handles { h.join().unwrap(); }
```

### Metrics

```rust
use ax_cache::Cache;

let cache: Cache<u64, u64> = Cache::new(10_000);
// ... use cache ...

let m = cache.metrics();
let total = m.hits + m.misses;
if total > 0 {
    println!("hit ratio: {:.2}%", m.hits as f64 * 100.0 / total as f64);
}
println!("inserted={} evicted={} rejected={}",
    m.insertions, m.evictions, m.rejections);
```

---

## Performance

The numbers below come from running the benchmarks in this repository on
an Apple Silicon laptop. They are reproducible — run `cargo bench` to
verify on your machine. **Performance varies with workload, hardware,
and concurrency level**; treat these as ballpark figures, not
guarantees.

### Single-thread microbench (uncontended)

`cargo bench --bench single_thread` (`Cache<u64, u64>`):

| Operation       | Latency  | Throughput     |
|-----------------|---------:|---------------:|
| `get_hit`       | ~8 ns    | ~120 Mops/s    |
| `get_miss`      | ~14 ns   | ~70 Mops/s     |
| `insert_update` | ~12 ns   | ~85 Mops/s     |
| `insert_new`    | ~70 ns   | ~14 Mops/s     |

`insert_new` is dominated by allocation, hashing, queue admission, and
(on overflow) eviction work; `insert_update` only mutates an existing
slot.

### Contention sweep (Zipfian, write-heavy)

`cargo bench --bench contention` with `cap=1M, universe=10M, zipf α=0.99, 5% writes`:

| Threads | Throughput   | P50    | P99     | Hit ratio |
|--------:|-------------:|-------:|--------:|----------:|
| 1       | ~9 Mops/s    | ~42 ns | ~250 ns |   ~0.83   |
| 4       | ~30 Mops/s   | ~83 ns | ~290 ns |   ~0.84   |
| 8       | ~35 Mops/s   | ~125 ns| ~420 ns |   ~0.84   |

### 30-minute soak (realistic workload)

8 threads, `Cache<u64, Arc<[u8]>>` with 256-byte payloads, `cap=1M`,
`universe=10M`, zipf α=0.99, 5% writes:

| Metric          | Value             |
|-----------------|-------------------|
| Throughput      | 11.4 Mops/s       |
| Hit ratio       | 0.84              |
| P50             | 500 ns            |
| P99             | 29 µs             |
| P999            | 101 µs            |
| Peak RSS        | ~150 MB           |
| Final entries   | 1,000,000         |

The gap between the contention bench (`u64` values, in-memory tight
loop) and the soak test (`Arc<[u8]>` payloads, longer run) reflects
DRAM bandwidth and OS scheduler effects on long-running workloads. P99+
spikes in the soak test are dominated by OS scheduling, not cache code.

### Hit ratio vs Zipfian skew

| α (skew)        | Hit ratio |
|-----------------|----------:|
| 0.99 (typical)  | ~0.84     |
| 1.2 (heavy)     | ~0.96     |

Hit ratio scales with workload skew: the more concentrated the access
pattern on a few hot keys, the more effectively S3-FIFO + TinyLFU
preserves them.

### Scan resistance

`cargo bench --bench scan_resistance` (`cap=10k`, 1k hot keys + 100k
scan keys): hot-key survival rate is **~90–100%** depending on
admission pressure. Cold scan keys are dropped from the small queue
within a handful of evictions; hot keys live in the main queue.

---

## When to use ax-cache

**Good fit:**

- Server-side hot-data caches with concentrated access patterns
  (Zipfian, power-law).
- Workloads that need bounded memory with predictable eviction.
- Scenarios where occasional one-hit-wonder reads should not pollute
  the cache (admission filtering helps).

**Less good fit:**

- Caches where every key matters equally and rejection is unacceptable
  (consider a plain `RwLock<HashMap>` or `dashmap`).
- Persistent / off-heap caches (this is in-memory only).
- Workloads dominated by very large values where memory bandwidth is
  the bottleneck rather than hash table operations.

---

## Limitations / honest caveats

- **`capacity` is a soft target.** The cache holds approximately
  `capacity` entries in steady state; it may briefly sit a few entries
  above capacity between inserts.
- **No async API.** All operations are synchronous and lock-based.
- **Values are cloned on `get`.** If `V` is expensive to clone, wrap it
  in `Arc<T>`.
- **Tail latency at high concurrency is OS-bound on the slowest
  percentiles.** P99 spikes in long-running workloads correlate with
  scheduler events (preemption, page faults), not with cache internals.
- **No serialization / persistence.** Drop the cache, lose the data.

---

## License

MIT — see [LICENSE](LICENSE).
