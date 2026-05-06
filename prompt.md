# AxCache System Design Prompt (STRICT EXECUTION SPEC)

## 🎯 OBJECTIVE

Build a **production-grade, ultra-high performance in-memory cache system** in Rust 2024 named **AxCache**.

Target:

- SET: ≥ 500K – 1M ops/sec
- GET: ≥ 1M ops/sec
- P99 latency: sub-millisecond
- Zero lock contention
- Zero unnecessary allocation in hot path

---

## 🧠 CORE PRINCIPLES (NON-NEGOTIABLE)

### 1. Zero-Tax Architecture

- Every CPU cycle must contribute to data processing
- No redundant work in hot path
- No unnecessary memory copy, allocation, or locking

---

### 2. Shared-Nothing + Thread-Per-Core (TPC)

- 1 worker thread = 1 dedicated CPU core
- Each shard:
  - owns its data exclusively
  - has its own allocator
  - has its own IO ring

- ❌ NO shared state between threads
- Communication ONLY via message passing (SPSC queue)

---

### 3. Hot Path Must Be Minimal

**CRITICAL RULE:**

```
SET:
  insert → DONE

GET:
  lookup → DONE
```

❌ Forbidden in hot path:

- eviction loops
- multi-structure updates
- heavy allocation
- scanning large structures
- blocking operations

---

## ⚠️ CRITICAL PERFORMANCE RULES

### ❌ DO NOT:

- run eviction inside insert()
- clone keys unnecessarily
- maintain duplicate ownership of data
- update multiple hash maps per operation
- perform loops inside write path
- use global allocator in hot path

---

### ✅ MUST:

- single ownership of data
- use references / index / pointer
- minimize memory movement
- ensure cache locality
- amortize expensive work

---

## 🧩 DATA ARCHITECTURE

### Key Storage

- Avoid `String` cloning
- Use:
  - `Arc<[u8]>` OR
  - slab-allocated buffer OR
  - indexed storage

---

### Value Storage

- Inline small values (≤64 bytes)
- Avoid heap allocation per request
- Prefer slab allocation

---

### Hashing

- Use AxHash (high-speed, high entropy)
- Pre-hash keys where possible

---

### Hash Table

- SIMD-optimized (SwissTable style)
- Open addressing
- Metadata fingerprint scanning

---

## 🧠 MEMORY MODEL

### Slab Allocation

- Pre-allocated memory pools
- Fixed-size slots
- No malloc/free in hot path

---

### Zero Copy

- No serialization/deserialization in critical path
- Direct memory access when possible

---

## 🔄 EVICTION STRATEGY

### Supported Algorithms:

- S3-FIFO
- SIEVE

---

### CRITICAL CONSTRAINT:

❌ Eviction MUST NOT run inside insert()

---

### Correct Model:

```
insert → fast write
↓
background worker / heartbeat
↓
incremental eviction
```

---

### Rules:

- eviction must be:
  - incremental
  - bounded work per cycle
  - amortized

- never block write path

---

## ⚡ CONCURRENCY MODEL

### Inter-thread Communication

- Wait-free SPSC queue only
- No mutex
- No spinlock

---

### Queue Constraints

- cache-line padded
- power-of-two capacity
- no dynamic allocation

---

## 🔥 I/O MODEL

- Use io_uring (completion-based)
- Avoid epoll-style readiness model
- Batch operations
- Zero-copy networking where possible

---

## 📉 PERFORMANCE ANTI-PATTERNS (STRICTLY FORBIDDEN)

If any of these appear, system is considered INVALID:

- eviction inside insert loop
- multiple hashmap removes per SET
- cloning keys in hot path
- pointer chasing across multiple structures
- frequent malloc/free
- global lock or shared state
- heavy logic inside GET

---

## 📊 PERFORMANCE PRIORITY ORDER

1. Reduce work per operation
2. Optimize data layout
3. Reduce memory allocation
4. Improve cache locality
5. THEN apply SIMD

---

## 🧠 DESIGN PHILOSOPHY

> A simpler algorithm with low execution cost
> beats a smarter algorithm with high overhead.

---

## 🎯 FINAL GOAL

AxCache must:

- outperform Redis
- compete with DragonflyDB
- maintain strict memory and CPU efficiency
- scale linearly with CPU cores

---

## 🚫 FAILURE CONDITION

If system shows:

- low ops/sec despite SIMD
- high CPU usage per operation
- large gap between GET and SET

→ root cause is excessive work in hot path

---

## ✅ SUCCESS CONDITION

- SET path ≈ constant time, minimal instructions
- GET path ≈ direct lookup, no extra logic
- background tasks handle all heavy operations

---

```doc
axcache/
├── Cargo.toml
│   └── Root workspace manifest (LTO, opt-level=3, panic=abort)
│
├── crates/
│   ├── axcache-axhash/
│   │   ├── src/
│   │   │   ├── folded_mul.rs
│   │   │   │   └── Implementasi bit-folding u64 x u64 -> u128
│   │   │   └── lib.rs
│   │   │       └── Entry point AxHash dengan state RandomState untuk anti-HashDoS
│   │   └── Mesin hashing 65 GiB/s. Isolasi total dari I/O
│   │
│   ├── axcache-alloc/
│   │   ├── src/
│   │   │   ├── slab.rs
│   │   │   │   └── Logika Slab Allocation untuk objek ukuran tetap (cache line aligned)
│   │   │   └── lib.rs
│   │   │       └── Wrapper mimalloc/snmalloc via #[global_allocator]
│   │   └── Custom allocator. Mem-bypass glibc malloc
│   │
│   ├── axcache-store/
│   │   ├── src/
│   │   │   ├── dashtable.rs
│   │   │   │   └── Implementasi open addressing hash map
│   │   │   ├── simd_scan.rs
│   │   │   │   └── Modul pencarian metadata 16/32 bit menggunakan core::simd
│   │   │   └── shard.rs
│   │   │       └── Representasi partisi data privat per-core
│   │   └── Core penyimpanan data (Shared-Nothing)
│   │
│   ├── axcache-evict/
│   │   ├── src/
│   │   │   ├── s3_fifo.rs
│   │   │   │   └── Logika 3-Queue (Small, Main, Ghost) demotion/promotion
│   │   │   └── sieve.rs
│   │   │       └── Implementasi evaluasi lazy bit "visited"
│   │   └── Algoritma eviksi memori (Tanpa Lock)
│   │
│   ├── axcache-io/
│   │   ├── src/
│   │   │   ├── net.rs
│   │   │   │   └── Wrapper io_uring TCP listener & socket
│   │   │   ├── buffer.rs
│   │   │   │   └── Manajemen ring buffer statis untuk I/O kernel
│   │   │   └── snapshot.rs
│   │   │       └── Logika Fork-less disk snapshotting asinkron
│   │   └── Manajemen asinkron dan networking kernel
│   │
│   └── axcache-engine/
│       ├── src/
│       │   ├── worker.rs
│       │   │   └── Loop event TPC, thread pinning (core affinity)
│       │   ├── spsc.rs
│       │   │   └── Wait-free message passing lintas worker
│       │   └── protocol.rs
│       │       └── Parser command klien (zero-copy dengan rkyv)
│       └── Orkestrator, protokol, dan worker management
│
└── src/
    └── main.rs
        └── Bootstrapping CLI, inisialisasi hardware, spawn TPC workers
```
