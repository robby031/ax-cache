// crates/axcache-store/src/dashtable.rs

use crate::simd_scan::match_fingerprint;
use axcache_axhash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

/// Kapasitas standar satu grup (bucket) yang dioptimalkan untuk SIMD 128-bit
const GROUP_SIZE: usize = 16;
const EMPTY_FINGERPRINT: u8 = 0x00;

/// Struktur metadata terpisah untuk cache locality yang maksimal
#[derive(Clone, Copy)]
pub struct GroupMetadata {
    // Array 16 byte menyimpan 1 byte (fingerprint) dari setiap kunci di grup ini.
    // Memungkinkan pemindaian SIMD O(1) yang super cepat.
    fingerprints: [u8; GROUP_SIZE],
}

impl GroupMetadata {
    pub fn new() -> Self {
        Self {
            fingerprints: [EMPTY_FINGERPRINT; GROUP_SIZE],
        }
    }
}

/// Implementasi inti Dashtable
pub struct DashTable<K, V> {
    metadata: Vec<GroupMetadata>,
    // Data riil (Key, Value) idealnya akan diatur menggunakan axcache-alloc (Slab).
    // Untuk representasi pondasi awal, kita gunakan alokasi sekuensial.
    entries: Vec<Option<(K, V)>>,
    hasher: RandomState,
    capacity_mask: usize,
}

impl<K: Hash + Eq, V> DashTable<K, V> {
    pub fn new(capacity_groups: usize) -> Self {
        let capacity = capacity_groups.next_power_of_two();
        Self {
            metadata: vec![GroupMetadata::new(); capacity],
            entries: (0..(capacity * GROUP_SIZE)).map(|_| None).collect(),
            hasher: RandomState::new(),
            capacity_mask: capacity - 1,
        }
    }

    /// Mencari nilai berdasarkan kunci menggunakan instruksi SIMD
    pub fn get(&self, key: &K) -> Option<&V> {
        let hash = self.hash_key(key);

        // 1. Tentukan indeks grup dari 57 bit atas (H1)
        let group_idx = (hash >> 7) as usize & self.capacity_mask;

        // 2. Ambil 7 bit bawah sebagai fingerprint (H2), setel bit MSB agar tidak nol
        let fingerprint = (hash & 0x7F | 0x80) as u8;

        let group_meta = &self.metadata[group_idx];

        // 3. Scan 16 slot sekaligus dalam 1 siklus CPU!
        let mut bitmask = match_fingerprint(&group_meta.fingerprints, fingerprint);

        // 4. Iterasi hanya pada slot yang bitmask-nya 1 (sangat jarang terjadi kolisi)
        while bitmask != 0 {
            // Dapatkan indeks bit pertama yang bernilai 1
            let bit_idx = bitmask.trailing_zeros() as usize;

            // Hitung indeks flat asli untuk entries
            let entry_idx = (group_idx * GROUP_SIZE) + bit_idx;

            if let Some((k, v)) = &self.entries[entry_idx] {
                if k == key {
                    return Some(v); // Kunci benar-benar cocok!
                }
            }

            // Hapus bit yang sudah dicek
            bitmask &= bitmask - 1;
        }

        None
    }
    pub fn insert(&mut self, key: K, value: V) {
        let hash = self.hash_key(&key);

        // 1. Tentukan indeks grup (H1) dan fingerprint (H2)
        let group_idx = (hash >> 7) as usize & self.capacity_mask;
        let fingerprint = (hash & 0x7F | 0x80) as u8;

        let group_meta = &mut self.metadata[group_idx];

        // 2. Cari slot kosong di dalam grup (linear probing internal grup)
        // Untuk optimasi maksimal, ini juga bisa menggunakan SIMD scan untuk mencari EMPTY_FINGERPRINT
        for bit_idx in 0..GROUP_SIZE {
            if group_meta.fingerprints[bit_idx] == EMPTY_FINGERPRINT {
                // 3. Simpan sidik jari di metadata agar bisa ditemukan oleh SIMD get()
                group_meta.fingerprints[bit_idx] = fingerprint;

                // 4. Hitung indeks flat dan simpan data riil
                let entry_idx = (group_idx * GROUP_SIZE) + bit_idx;
                self.entries[entry_idx] = Some((key, value));
                return;
            }
        }

        // Catatan: Jika grup ini penuh (collision), sistem produksi memerlukan logika
        // 'Probing' ke grup tetangga atau mekanisme 'Resize'.
        // Untuk tahap MVP ini, kita asumsikan kapasitas awal mencukupi.
    }

    pub fn remove(&mut self, key: &K) -> bool {
        let hash = self.hash_key(key);

        // 1. Hitung indeks grup (H1) dan fingerprint (H2)
        let group_idx = (hash >> 7) as usize & self.capacity_mask;
        let fingerprint = (hash & 0x7F | 0x80) as u8;

        let group_meta = &mut self.metadata[group_idx];

        // 2. Scan metadata menggunakan SIMD untuk mencari slot yang mungkin cocok
        let mut bitmask = match_fingerprint(&group_meta.fingerprints, fingerprint);

        // 3. Iterasi slot yang ditemukan oleh SIMD
        while bitmask != 0 {
            let bit_idx = bitmask.trailing_zeros() as usize;
            let entry_idx = (group_idx * GROUP_SIZE) + bit_idx;

            if let Some((k, _)) = &self.entries[entry_idx] {
                if k == key {
                    // --- LOGIKA PENGHAPUSAN KRUSIAL ---

                    // A. Hapus data dari entries (membebaskan memori)
                    self.entries[entry_idx] = None;

                    // B. RESET METADATA ke status EMPTY (0x00)
                    // Sangat penting agar pemindaian SIMD berikutnya melewati slot ini[cite: 1196].
                    group_meta.fingerprints[bit_idx] = EMPTY_FINGERPRINT;

                    return true; // Berhasil dihapus
                }
            }

            // Hapus bit yang sudah dicek untuk lanjut ke kandidat berikutnya
            bitmask &= bitmask - 1;
        }

        false // Kunci tidak ditemukan
    }

    fn hash_key(&self, key: &K) -> u64 {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    }
}
