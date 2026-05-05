// crates/axcache-store/src/lib.rs

// Memerlukan fitur nightly untuk instruksi core::simd
#![feature(portable_simd)]
pub mod dashtable;
pub mod shard;
pub mod simd_scan;
