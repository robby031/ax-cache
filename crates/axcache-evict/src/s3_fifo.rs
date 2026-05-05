// crates/axcache-evict/src/s3_fifo.rs

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

/// Struktur yang memisahkan status akses dari data riil,
/// sehingga eviksi sangat ringan dan hemat memori.
pub struct S3Fifo<K> {
    // Menyaring objek baru (one-hit wonders)
    small_q: VecDeque<(K, bool)>,
    // Menyimpan objek populer yang terbukti diakses lebih dari sekali
    main_q: VecDeque<(K, bool)>,
    // Hanya menyimpan metadata (Key) dari objek yang baru dieviksi
    ghost_q: VecDeque<K>,
    ghost_set: HashSet<K>,

    small_capacity: usize,
    main_capacity: usize,
    ghost_capacity: usize,
}

impl<K: Hash + Eq + Clone> S3Fifo<K> {
    pub fn new(capacity: usize) -> Self {
        // Alokasi standar S3-FIFO: 10% untuk Small, 90% untuk Main
        let small_cap = (capacity as f64 * 0.1).max(1.0) as usize;
        let main_cap = capacity - small_cap;
        let ghost_cap = main_cap;

        Self {
            small_q: VecDeque::with_capacity(small_cap),
            main_q: VecDeque::with_capacity(main_cap),
            ghost_q: VecDeque::with_capacity(ghost_cap),
            ghost_set: HashSet::with_capacity(ghost_cap),
            small_capacity: small_cap,
            main_capacity: main_cap,
            ghost_capacity: ghost_cap,
        }
    }

    /// Menandai kunci sebagai telah diakses (visited = true).
    pub fn read(&mut self, key: &K) {
        // MVP: Pencarian linier. Pada fase tuning nanti,
        // bit 'visited' akan digeser langsung ke metadata Dashtable untuk pencarian O(1).
        for entry in self.small_q.iter_mut() {
            if &entry.0 == key {
                entry.1 = true;
                return;
            }
        }
        for entry in self.main_q.iter_mut() {
            if &entry.0 == key {
                entry.1 = true;
                return;
            }
        }
    }

    /// Memasukkan kunci baru dan mengembalikan daftar kunci yang harus diusir dari cache.
    pub fn insert(&mut self, key: K) -> Vec<K> {
        let mut evicted_keys = Vec::new();

        // 1. Jika antrian Small penuh, eviksi!
        while self.small_q.len() >= self.small_capacity {
            if let Some(evicted_from_small) = self.evict_small() {
                evicted_keys.push(evicted_from_small);
            }
        }

        // 2. Jika antrian Main penuh, eviksi!
        while self.main_q.len() >= self.main_capacity {
            if let Some(evicted_from_main) = self.evict_main() {
                evicted_keys.push(evicted_from_main);
            }
        }

        // 3. Masukkan kunci baru
        if self.ghost_set.contains(&key) {
            // Objek populer yang hilang (Pernah masuk Ghost Queue), langsung promosi ke Main!
            self.ghost_set.remove(&key);
            self.main_q.push_back((key, false));
        } else {
            // Objek sepenuhnya baru, saring dulu di Small Queue
            self.small_q.push_back((key, false));
        }

        evicted_keys
    }

    fn evict_small(&mut self) -> Option<K> {
        if let Some((key, visited)) = self.small_q.pop_front() {
            if visited {
                // Dipromosikan ke Main Queue!
                self.main_q.push_back((key, false));
                None
            } else {
                // Diusir. Masukkan metadata ke Ghost Queue.
                self.insert_ghost(key.clone());
                Some(key) // Beritahu Shard untuk menghapus kunci ini
            }
        } else {
            None
        }
    }

    fn evict_main(&mut self) -> Option<K> {
        while let Some((key, visited)) = self.main_q.pop_front() {
            if visited {
                // Masih populer, turunkan statusnya dan kembalikan ke antrian
                self.main_q.push_back((key, false));
            } else {
                // Benar-benar usang, usir dari sistem.
                return Some(key);
            }
        }
        None
    }

    fn insert_ghost(&mut self, key: K) {
        if self.ghost_q.len() >= self.ghost_capacity {
            if let Some(old_key) = self.ghost_q.pop_front() {
                self.ghost_set.remove(&old_key);
            }
        }
        self.ghost_set.insert(key.clone());
        self.ghost_q.push_back(key);
    }
}
