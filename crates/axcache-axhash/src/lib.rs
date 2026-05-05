// crates/axcache-axhash/src/lib.rs

pub mod folded_mul;

use axhash::{Hasher, hash_value};
use rand::Rng;
use std::hash::BuildHasher as StdBuildHasher;

/// RandomState khusus untuk AxCache.
/// Menginisialisasi seed acak saat AxCache pertama kali dijalankan (runtime)
/// untuk mencegah serangan HashDoS yang menargetkan kolisi prediktif.
#[derive(Clone)]
pub struct RandomState {
    seed: u64,
}

impl RandomState {
    pub fn new() -> Self {
        let mut rng = rand::rng();
        let seed = rng.next_u64();
        Self { seed }
    }
}

impl Default for RandomState {
    fn default() -> Self {
        Self::new()
    }
}

impl StdBuildHasher for RandomState {
    type Hasher = Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        // Menggunakan instance Hasher dengan seed yang sudah kita amankan
        axhash::BuildHasher::with_seed(self.seed).build_hasher()
    }
}

/// Helper function untuk menge-hash struktur data secara langsung (mirip dengan doc axhash).
#[inline]
pub fn hash_item<T: std::hash::Hash>(item: &T) -> u64 {
    hash_value(item)
}
