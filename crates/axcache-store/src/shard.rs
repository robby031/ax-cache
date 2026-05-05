// crates/axcache-store/src/shard.rs

use crate::dashtable::DashTable;
use axcache_evict::s3_fifo::S3Fifo;
use std::fmt::Debug;
/// Shard privat eksklusif untuk satu core (No Locking)
pub struct Shard<K, V> {
    pub core_id: usize,
    table: DashTable<K, V>, // Untuk MVP, Anda bisa menggunakan std::collections::HashMap sebagai fallback jika Dashtable belum lengkap
    eviction_policy: S3Fifo<K>,
}

impl<K: std::hash::Hash + Eq + Clone + Debug, V> Shard<K, V> {
    pub fn new(core_id: usize, capacity: usize) -> Self {
        Self {
            core_id,
            table: DashTable::new(capacity), // Atau HashMap::with_capacity(capacity) untuk MVP cepat
            eviction_policy: S3Fifo::new(capacity),
        }
    }

    /// Operasi Get
    #[inline]
    pub fn get(&mut self, key: &K) -> Option<&V> {
        // Cari menggunakan pemindaian SIMD paralel
        let res = self.table.get(key);
        if res.is_some() {
            self.eviction_policy.read(key);
        }
        res
    }

    /// Operasi Set dengan Auto-Eviction
    #[inline]
    pub fn set(&mut self, key: K, value: V) {
        // 1. Daftarkan ke kebijakan eviksi S3-FIFO
        let evicted_keys = self.eviction_policy.insert(key.clone());

        // 2. Eksekusi instruksi eviksi untuk membebaskan memori
        for evicted in evicted_keys {
            self.table.remove(&evicted);
            println!("Core {}: 🗑️ Kunci '{:?}' diusir", self.core_id, evicted);
        }

        // 3. MASUKKAN DATA KE DASHTABLE (Langkah krusial!)
        self.table.insert(key, value);
    }
}
