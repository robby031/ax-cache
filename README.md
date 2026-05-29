# ax-cache

Concurrent in-memory cache for Rust with admission-controlled eviction.

- Sharded architecture
- S3-FIFO eviction
- TinyLFU admission filtering
- TTL support
- Thread-safe design

[![Crates.io](https://img.shields.io/crates/v/ax-cache.svg)](https://crates.io/crates/ax-cache)
[![Docs.rs](https://docs.rs/ax-cache/badge.svg)](https://docs.rs/ax-cache)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Installation

```toml
[dependencies]
ax-cache = "1.0"
```

---

## Quick Start

```rust
use ax_cache::Cache;

let cache: Cache<String, u64> = Cache::new(10_000);

cache.insert("alpha".to_string(), 1);

assert_eq!(cache.get("alpha"), Some(1));
```

---

## Construction

```rust
use ax_cache::Cache;

// Auto shard count
let cache = Cache::<u64, u64>::new(100_000);

// Explicit shard count
let cache = Cache::<u64, u64>::with_shards(100_000, 16);
```

---

## TTL Support

```rust
use ax_cache::Cache;
use std::time::Duration;

let cache = Cache::<String, String>::new(10_000);

cache.insert_with_ttl(
    "session".to_string(),
    "token".to_string(),
    Duration::from_secs(60),
);
```

Expired entries are reclaimed lazily on access.

---

## Shared Across Threads

```rust
use ax_cache::Cache;
use std::sync::Arc;
use std::thread;

let cache = Arc::new(Cache::<u64, u64>::new(100_000));

let handles: Vec<_> = (0..8).map(|t| {
    let cache = Arc::clone(&cache);

    thread::spawn(move || {
        for i in 0..10_000 {
            cache.insert(t * 100_000 + i, i);
        }
    })
}).collect();

for h in handles {
    h.join().unwrap();
}
```

---

## Metrics

```rust
use ax_cache::Cache;

let cache = Cache::<u64, u64>::new(10_000);

let metrics = cache.metrics();

println!("hits={}", metrics.hits);
println!("misses={}", metrics.misses);
println!("evictions={}", metrics.evictions);
```

---

## InsertOutcome

```rust
use ax_cache::{Cache, InsertOutcome};

let cache = Cache::<u64, u64>::new(10);

match cache.insert(1, 10) {
    InsertOutcome::Inserted => {}
    InsertOutcome::Updated => {}
    InsertOutcome::Rejected => {}
}
```

---

## Maintenance Thread

```rust
use ax_cache::{Cache, MaintenanceConfig};
use std::time::Duration;

let cache = Cache::<u64, Vec<u8>>::new(100_000);

cache.enable_maintenance(MaintenanceConfig {
    sweep_interval: Duration::from_millis(250),
    max_sweep_per_shard: 128,
});
```

Background maintenance stops automatically when the cache is dropped.

---

## Notes

- `capacity` is approximate, not strict.
- Operations are synchronous.
- `get()` clones values. Use `Arc<T>` for large payloads.
- In-memory only. No persistence or serialization.

---

## Links

- Crate: https://crates.io/crates/ax-cache
- Docs: https://docs.rs/ax-cache
- hashbrown: https://crates.io/crates/hashbrown
- parking_lot: https://crates.io/crates/parking_lot
- axhash: https://crates.io/crates/axhash-core

---

## License

MIT