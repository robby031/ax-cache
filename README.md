# ax-cache

Hardware-aware concurrent cache engine for Rust.

**Sharded SwissTable** (via [hashbrown]) · **S3-FIFO eviction** · **TinyLFU admission** · **Fueled by [axhash]** (AES-NI accelerated hashing)

[![Crates.io](https://img.shields.io/crates/v/ax-cache.svg)](https://crates.io/crates/ax-cache)
[![Docs.rs](https://docs.rs/ax-cache/badge.svg)](https://docs.rs/ax-cache)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why ax-cache?

Most Rust caches either sacrifice concurrency for simplicity, or sacrifice latency predictability for raw throughput. `ax-cache` is designed from the ground up to do both:

- **Read path holds only a shared lock** — no queue mutation on hit, just one atomic frequency bump
- **S3-FIFO eviction** outperforms LRU with lower metadata overhead and better scan resistance
- **TinyLFU admission** filters out one-hit-wonders, reducing evictions by 68–94%
- **Per-shard metrics** on separate cache lines eliminate false sharing
- **Fibonacci shard routing** decorrelates shard selection from hashbrown's internal h1/h2 probing

The result: **8.6 ns get-hit latency**, **116 Mops/s** single-thread reads, scaling to **34+ Mops/s** at 8 threads with **96%+ hit ratio** under Zipfian workloads.

## Ecosystem

| Crate             | Description                                 |
| ----------------- | ------------------------------------------- |
| `axhash`          | High-performance AES-NI hashing engine      |
| `axhash-map`      | Fast HashMap/HashSet powered by `hashbrown` |
| `axhash-indexmap` | Ordered maps with AxHash                    |
| `axhash-dashmap`  | Concurrent DashMap powered by AxHash        |
| **`ax-cache`**    | **Hardware-aware concurrent cache engine**  |

```text
┌──────────────────────────────────────────────────────────┐
│                        ax-cache                          │
│                                                          │
│   Cache<K, V>                                            │
│   ├── Sharded SwissTable (hashbrown + AxHasher)          │
│   ├── S3-FIFO eviction (small → main promotion)          │
│   ├── TinyLFU admission (Count-Min Sketch gate)          │
│   ├── Optional TTL with 1ms resolution                   │
│   ├── Background maintenance worker                      │
│   └── Per-shard atomic metrics                           │
│                                                          │
│   Read path:  shared lock → hash lookup → freq bump      │
│   Write path: exclusive lock → sketch → admit/reject     │
│                                                          │
│   Shard routing: Fibonacci hash (top bits)               │
│   Hashing: BuildHasherDefault<AxHasher> (AES-NI)         │
└──────────────────────────────────────────────────────────┘
```

---

## Benchmark Results

Measured on Apple Silicon M4 (`release` build).

### Single-thread latency

| Operation                  | Latency |     Throughput |
| -------------------------- | ------: | -------------: |
| `get` — cache hit          |  8.6 ns | **116 Mops/s** |
| `get` — cache miss         | 13.7 ns |      73 Mops/s |
| `insert` — new key         |   72 ns |      14 Mops/s |
| `insert` — update existing |   10 ns |      96 Mops/s |

### Multi-thread contention (Zipf α=0.99, 95% read / 5% write)

| Threads |  Throughput |    p50 |    p99 | Hit Ratio |
| ------: | ----------: | -----: | -----: | --------: |
|       1 | 10.1 Mops/s |  42 ns | 209 ns |     81.0% |
|       2 | 18.8 Mops/s |  42 ns | 209 ns |     82.2% |
|       4 | 26.0 Mops/s |  83 ns | 334 ns |     83.2% |
|       8 | 34.7 Mops/s | 125 ns | 417 ns |     83.8% |

### Hit ratio & scan resistance

| Workload                       |         Hit Ratio | Evictions |
| ------------------------------ | ----------------: | --------: |
| Zipfian α=1.2 (95/5 R/W)       |         **96.4%** |       324 |
| Hot+Scan (1K hot, 100K scan)   | 100% hot survival |    29,151 |
| 3-tier (hot/warm/cold 60/35/5) |             75.4% |   243,387 |

### Head-to-head comparison

All sync variants, cycling keys, Apple Silicon M4, `release` build.

**Single-thread latency (2M iterations)**

| Benchmark        | ax-cache    | quick_cache | mini-moka   |
| ---------------- | ----------: | ----------: | ----------: |
| `get` (hit)      |    9.4 ns   |    6.7 ns   |  104.7 ns   |
| `insert` (new)   |   78.7 ns   |   50.7 ns   |  303.7 ns   |

**Multi-thread throughput (Zipf α=0.99, 95% read / 5% write)**

| Threads | ax-cache     | quick_cache  | mini-moka    |
| ------: | -----------: | -----------: | -----------: |
|       1 | 20.9 Mops/s  | 22.2 Mops/s  |  5.3 Mops/s  |
|       4 | 45.5 Mops/s  | 69.4 Mops/s  | 10.1 Mops/s  |
|       8 | 51.8 Mops/s  | 70.9 Mops/s  |  5.8 Mops/s  |

> **Note:** `quick_cache` leads on raw throughput; `ax-cache` differentiates
> on **hit ratio** via S3-FIFO eviction + TinyLFU admission — see the hit
> ratio & scan resistance table above. Both are far ahead of `mini-moka`.

Run the benchmarks yourself:

```bash
cargo bench --bench single_thread
cargo bench --bench contention
cargo bench --bench zipfian
cargo bench --bench scan_resistance
```

---

## Installation

```toml
[dependencies]
ax-cache = "0.1"
```

No feature flags required. AES acceleration is detected at runtime; a portable
fallback is used automatically on CPUs without AES instructions.

---

## Quick start

### Basic usage

```rust
use ax_cache::Cache;

// Create a cache with ~10,000 entry capacity.
// Shard count is auto-tuned based on available CPU parallelism.
let cache: Cache<String, Vec<u8>> = Cache::new(10_000);

// Insert (key must be owned)
cache.insert("user:1001".to_string(), vec![1, 2, 3]);

// Lookup with &str — no allocation, no .into() needed.
if let Some(data) = cache.get("user:1001") {
    println!("found: {} bytes", data.len());
}

// Remove with &str
let old = cache.remove("user:1001");
```

### With TTL (time-to-live)

```rust
use ax_cache::Cache;
use std::time::Duration;

let cache: Cache<String, String> = Cache::new(50_000);

// Entry expires after 30 seconds.
cache.insert_with_ttl(
    "session:abc".to_string(),
    "token_data".to_string(),
    Duration::from_secs(30),
);

// Lookup with &str — no allocation needed.
assert!(cache.get("session:abc").is_some());

std::thread::sleep(Duration::from_secs(31));
assert!(cache.get("session:abc").is_none());
```

### With background maintenance

```rust
use ax_cache::{Cache, MaintenanceConfig};
use std::time::Duration;

let cache: Cache<u64, Vec<u8>> = Cache::new(100_000);

// No `mut` needed — enable_maintenance takes &self.
cache.enable_maintenance(MaintenanceConfig {
    sweep_interval: Duration::from_millis(250),
    max_sweep_per_shard: 128,
});

// Use cache normally — expired entries are cleaned up in the background.
cache.insert_with_ttl(42, vec![0u8; 1024], Duration::from_secs(5));

// The maintenance thread stops automatically when the cache is dropped.
```

### Observability with metrics

```rust
use ax_cache::Cache;

let cache: Cache<u64, u64> = Cache::new(10_000);

// ... use cache ...
for i in 0..1000u64 {
    cache.insert(i, i * 10);
}
for i in 0..500u64 {
    let _ = cache.get(&i);
}

let m = cache.metrics();
println!("hits:       {}", m.hits);
println!("misses:     {}", m.misses);
println!("insertions: {}", m.insertions);
println!("evictions:  {}", m.evictions);
println!("rejections: {}", m.rejections);  // TinyLFU filtered

let hit_ratio = m.hits as f64 / (m.hits + m.misses).max(1) as f64;
println!("hit ratio:  {:.2}%", hit_ratio * 100.0);
```

### Multi-threaded usage

```rust
use ax_cache::Cache;
use std::sync::Arc;
use std::thread;

// Cache is Send + Sync — wrap in Arc for shared ownership.
let cache = Arc::new(Cache::<u64, u64>::new(100_000));

let mut handles = vec![];
for t in 0..8u64 {
    let c = Arc::clone(&cache);
    handles.push(thread::spawn(move || {
        for i in 0..10_000u64 {
            let key = t * 100_000 + i;
            c.insert(key, key * 2);
            let _ = c.get(&key);
        }
    }));
}

for h in handles {
    h.join().unwrap();
}

let m = cache.metrics();
println!("{} ops, hit ratio {:.1}%",
    m.hits + m.misses,
    m.hits as f64 / (m.hits + m.misses).max(1) as f64 * 100.0,
);
```

---

## API Reference

### Constructors

```rust
use ax_cache::{Cache, MaintenanceConfig};
use std::time::Duration;

// Auto-tuned shard count (4 × logical CPUs, rounded to power of two).
let cache: Cache<String, i64> = Cache::new(100_000);

// Explicit shard count (rounded up to next power of two).
let cache: Cache<String, i64> = Cache::with_shards(100_000, 32);

// Enable background maintenance — no `mut` binding required.
let cache: Cache<String, i64> = Cache::new(100_000);
cache.enable_maintenance(MaintenanceConfig::default());
```

| Constructor                                 | Description                                    |
| ------------------------------------------- | ---------------------------------------------- |
| `Cache::new(capacity)`                      | Auto-tuned shards based on CPU count           |
| `Cache::with_shards(capacity, shard_count)` | Explicit shard count (rounded to power of two) |

### Core operations

| Method                                    | Lock              | Description                                                                    |
| ----------------------------------------- | ----------------- | ------------------------------------------------------------------------------ |
| `get<Q>(&key) → Option<V>`                | Shared (read)     | Lookup + frequency bump. Accepts borrowed key (e.g. `&str` for `String` keys). |
| `insert(key, value) → bool`               | Exclusive (write) | Insert without TTL. Returns `true` if new, `false` if updated.                 |
| `insert_with_ttl(key, value, ttl) → bool` | Exclusive (write) | Insert with expiry. 1 ms resolution, max ~49.7 days.                           |
| `remove<Q>(&key) → Option<V>`             | Exclusive (write) | Remove and return value. Accepts borrowed key. Does not check TTL.             |

### Introspection

| Method                        | Description                                                                 |
| ----------------------------- | --------------------------------------------------------------------------- |
| `len() → usize`               | Approximate entry count across all shards (includes expired-not-yet-swept). |
| `is_empty() → bool`           | Whether the cache holds zero entries.                                       |
| `shard_count() → usize`       | Number of shards (always power of two).                                     |
| `metrics() → MetricsSnapshot` | Aggregated counters across all shards.                                      |

### Maintenance

| Method                       | Description                                              |
| ---------------------------- | -------------------------------------------------------- |
| `enable_maintenance(config)` | Spawn a background thread for proactive expiry sweeping. |

The maintenance thread is stopped automatically when the `Cache` is dropped.

---

## Type constraints

```rust
Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
```

- **`K: Clone`** — keys are cloned into the eviction policy queues.
- **`V: Clone`** — values are cloned on `get` (the read path holds a shared lock, so ownership can't be transferred).
- For `get` / `remove`: accepts `&Q` where `K: Borrow<Q>` — e.g. `&str` for `String` keys, `&[u8]` for `Vec<u8>` keys.
- For `enable_maintenance`, additionally: `K: Send + Sync + 'static`, `V: Send + Sync + 'static`.

---

## `MetricsSnapshot`

```rust
pub struct MetricsSnapshot {
    pub hits: u64,        // Successful get() calls
    pub misses: u64,      // get() calls that returned None
    pub insertions: u64,  // New entries admitted to the cache
    pub evictions: u64,   // Entries removed by S3-FIFO eviction
    pub rejections: u64,  // New entries rejected by TinyLFU admission
}
```

Metrics are collected per-shard using atomic counters on separate cache lines (no false sharing). The `metrics()` method aggregates across all shards into a single snapshot.

---

## `MaintenanceConfig`

```rust
pub struct MaintenanceConfig {
    /// How often the worker wakes up to sweep. Default: 500ms.
    pub sweep_interval: Duration,
    /// Max expired entries to sweep per shard per cycle. Default: 64.
    /// Bounds the time spent holding any single shard's write lock.
    pub max_sweep_per_shard: usize,
}
```

```rust
use ax_cache::MaintenanceConfig;
use std::time::Duration;

// Use defaults (500ms interval, 64 entries/shard/cycle).
let config = MaintenanceConfig::default();

// Aggressive sweeping for high-TTL-churn workloads.
let config = MaintenanceConfig {
    sweep_interval: Duration::from_millis(100),
    max_sweep_per_shard: 256,
};

// Gentle sweeping for low-TTL-churn workloads.
let config = MaintenanceConfig {
    sweep_interval: Duration::from_secs(5),
    max_sweep_per_shard: 32,
};
```

---

## TTL behavior

| Scenario                                         | Behavior                                                   |
| ------------------------------------------------ | ---------------------------------------------------------- |
| `insert(k, v)`                                   | Entry never expires automatically.                         |
| `insert_with_ttl(k, v, Duration::from_secs(30))` | Entry expires 30s after insertion.                         |
| `insert_with_ttl(k, v, Duration::ZERO)`          | Entry is immediately expired (next `get` returns `None`).  |
| Re-insert with new TTL                           | Deadline is reset to the new TTL.                          |
| `get` after expiry                               | Returns `None`. Entry is lazily reclaimed during eviction. |
| `remove` of expired entry                        | Returns `Some(value)` — removal does not check TTL.        |
| Mixed TTL / no-TTL entries                       | Fully supported in the same cache instance.                |

**Resolution:** 1 ms. **Maximum range:** ~49.7 days from cache construction.
Sub-millisecond TTLs round down; TTLs beyond 49.7 days are clamped.

---

Each shard is independently locked with `parking_lot::RwLock`:

- **Reads** take a shared lock — multiple threads read concurrently
- **Writes** take an exclusive lock — scoped to a single shard

### S3-FIFO eviction

```text
new entry ──→ [small queue] ──→ evict (freq == 0)
                    │
                    └── promote (freq > 0) ──→ [main queue]
                                                    │
                                                    ├── re-promote (freq > 0, decrement)
                                                    └── evict (freq == 0)
```

- **Small queue** (~10% of capacity): probation area for new entries
- **Main queue** (~90% of capacity): entries that earned at least one hit
- Expired entries (TTL) are dropped on sight regardless of frequency

### TinyLFU admission

When the cache is at capacity, new keys must pass a **Count-Min Sketch** frequency gate:

```text
insert(key) ──→ sketch.increment(hash)
            ──→ if at_capacity && sketch.estimate(hash) ≤ 1:
                    REJECT (one-hit-wonder)
                else:
                    ADMIT to small queue
```

This prevents scan workloads from displacing warm entries. In benchmarks, TinyLFU reduces evictions by **68–94%** across all tested workloads.

---

| Shard Count    | Best For                                         |
| -------------- | ------------------------------------------------ |
| Auto (default) | General-purpose, most applications               |
| 16–32          | Low-contention, moderate parallelism             |
| 64–128         | High-contention, many writer threads             |
| 1              | Testing, single-threaded, deterministic behavior |

---

## Dependency footprint

```text
ax-cache
├── axhash-core    (AxHasher — AES-NI hash engine)
├── hashbrown      (SwissTable — SIMD-accelerated probing)
└── parking_lot    (RwLock — fast reader-writer lock)
```

---

## License

MIT — see [LICENSE](LICENSE).

[hashbrown]: https://crates.io/crates/hashbrown
[axhash]: https://crates.io/crates/axhash
