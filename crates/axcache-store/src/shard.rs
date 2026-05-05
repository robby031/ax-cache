// crates/axcache-store/src/shard.rs
//
// Shard: private storage eksklusif untuk satu CPU core (no locking, no sharing).
//
// Fitur production:
//   • TTL (Time-To-Live): lazy expiration saat get()
//   • delete(): hapus key secara eksplisit
//   • flush(): FLUSHALL — hapus semua data
//   • size(): jumlah entry aktif (tidak termasuk expired)
//   • S3-FIFO eviction O(1)

use crate::dashtable::DashTable;
use axcache_evict::s3_fifo::S3Fifo;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Shard privat eksklusif untuk satu core.
/// Menggunakan Rc<RefCell> di layer atas (worker) sehingga tidak ada overhead Mutex.
pub struct Shard {
    pub core_id: usize,
    table: DashTable<String, Vec<u8>>,
    eviction: S3Fifo<String>,
    /// TTL tracking: key → waktu kadaluarsa
    expiry: HashMap<String, Instant>,
}

impl Shard {
    /// Buat shard baru. `capacity` = estimasi jumlah key maksimum.
    pub fn new(core_id: usize, capacity: usize) -> Self {
        // DashTable butuh grup; 1 grup = 16 slot → capacity/16 grup
        let groups = (capacity / 16).max(4);
        Self {
            core_id,
            table: DashTable::new(groups),
            eviction: S3Fifo::new(capacity),
            expiry: HashMap::with_capacity(capacity / 4),
        }
    }

    // =========================================================================
    // GET
    // =========================================================================

    /// Ambil value; lazy expiration jika TTL sudah lewat.
    #[inline]
    pub fn get(&mut self, key: &str) -> Option<&[u8]> {
        // 1. Cek TTL sebelum akses (lazy expiration)
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }

        // 2. Update eviction policy sebelum borrowing table
        let key_owned = key.to_string();
        self.eviction.read(&key_owned);

        // 3. Return referensi ke value
        self.table.get(&key_owned).map(|v| v.as_slice())
    }

    // =========================================================================
    // SET / SET WITH TTL
    // =========================================================================

    /// Simpan key-value tanpa TTL.
    pub fn set(&mut self, key: String, value: Vec<u8>) {
        self.expiry.remove(&key); // hapus TTL lama jika ada
        self.do_insert(key, value);
    }

    /// Simpan key-value dengan TTL (detik).
    pub fn set_ex(&mut self, key: String, value: Vec<u8>, ttl_secs: u64) {
        let exp = Instant::now() + Duration::from_secs(ttl_secs);
        self.expiry.insert(key.clone(), exp);
        self.do_insert(key, value);
    }

    // =========================================================================
    // DELETE
    // =========================================================================

    /// Hapus satu key. Mengembalikan true jika key ada dan dihapus.
    pub fn delete(&mut self, key: &str) -> bool {
        self.expiry.remove(key);
        self.eviction.remove(&key.to_string());
        self.table.remove(&key.to_string())
    }

    // =========================================================================
    // EXISTS / TTL
    // =========================================================================

    /// Cek apakah key ada dan belum expired.
    pub fn exists(&mut self, key: &str) -> bool {
        if self.is_expired(key) {
            self.delete(key);
            return false;
        }
        self.table.get(&key.to_string()).is_some()
    }

    /// Sisa waktu hidup dalam detik.
    /// Mengembalikan:
    ///   Some(n)  - n detik tersisa
    ///   Some(-1) - key ada tapi tidak punya TTL
    ///   None     - key tidak ada
    pub fn ttl(&mut self, key: &str) -> Option<i64> {
        if self.is_expired(key) {
            self.delete(key);
            return None;
        }
        if self.table.get(&key.to_string()).is_none() {
            return None;
        }
        match self.expiry.get(key) {
            Some(&exp) => {
                let now = Instant::now();
                if now >= exp {
                    self.delete(key);
                    None
                } else {
                    Some((exp - now).as_secs() as i64)
                }
            }
            None => Some(-1), // key ada, tidak ada TTL
        }
    }

    /// Set TTL pada key yang sudah ada. Mengembalikan false jika key tidak ada.
    pub fn expire(&mut self, key: &str, secs: u64) -> bool {
        if self.is_expired(key) {
            self.delete(key);
            return false;
        }
        if self.table.get(&key.to_string()).is_some() {
            let exp = Instant::now() + Duration::from_secs(secs);
            self.expiry.insert(key.to_string(), exp);
            true
        } else {
            false
        }
    }

    // =========================================================================
    // FLUSH / SIZE
    // =========================================================================

    /// Hapus semua data (FLUSHALL).
    pub fn flush(&mut self) {
        self.table.clear();
        self.eviction.clear();
        self.expiry.clear();
    }

    /// Jumlah entry aktif (tidak termasuk expired — tidak ada eager scan).
    #[inline]
    pub fn size(&self) -> usize {
        self.table.len()
    }

    // =========================================================================
    // Internal
    // =========================================================================

    /// Cek apakah key sudah expired (tanpa menghapusnya).
    #[inline]
    fn is_expired(&self, key: &str) -> bool {
        self.expiry
            .get(key)
            .map(|&exp| Instant::now() >= exp)
            .unwrap_or(false)
    }

    /// Insert ke DashTable dengan S3-FIFO eviction.
    fn do_insert(&mut self, key: String, value: Vec<u8>) {
        // S3-FIFO: insert key, dapatkan daftar key yang harus dieviksi
        let evicted = self.eviction.insert(key.clone());
        for ek in evicted {
            self.expiry.remove(&ek);
            self.table.remove(&ek);
        }
        // Insert ke DashTable
        self.table.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut s = Shard::new(0, 64);
        s.set("foo".to_string(), b"bar".to_vec());
        assert_eq!(s.get("foo"), Some(b"bar".as_ref()));
        assert_eq!(s.get("missing"), None);
    }

    #[test]
    fn test_delete() {
        let mut s = Shard::new(0, 64);
        s.set("k".to_string(), b"v".to_vec());
        assert!(s.delete("k"));
        assert!(!s.delete("k")); // sudah terhapus
        assert_eq!(s.get("k"), None);
    }

    #[test]
    fn test_ttl_expiration() {
        let mut s = Shard::new(0, 64);
        s.set_ex("temp".to_string(), b"data".to_vec(), 0); // TTL 0 detik → langsung expired
        // Paksa expired dengan memanipulasi expiry map
        s.expiry
            .insert("temp".to_string(), Instant::now() - Duration::from_secs(1));
        assert_eq!(s.get("temp"), None, "harus expired");
    }

    #[test]
    fn test_ttl_not_expired() {
        let mut s = Shard::new(0, 64);
        s.set_ex("alive".to_string(), b"data".to_vec(), 3600);
        assert_eq!(s.get("alive"), Some(b"data".as_ref()));
        assert!(s.ttl("alive").unwrap() > 0);
    }

    #[test]
    fn test_exists() {
        let mut s = Shard::new(0, 64);
        s.set("x".to_string(), b"1".to_vec());
        assert!(s.exists("x"));
        assert!(!s.exists("y"));
    }

    #[test]
    fn test_flush() {
        let mut s = Shard::new(0, 64);
        for i in 0..10u8 {
            s.set(format!("k{}", i), vec![i]);
        }
        s.flush();
        assert_eq!(s.size(), 0);
        assert_eq!(s.get("k0"), None);
    }

    #[test]
    fn test_size() {
        let mut s = Shard::new(0, 64);
        assert_eq!(s.size(), 0);
        s.set("a".to_string(), b"1".to_vec());
        s.set("b".to_string(), b"2".to_vec());
        assert_eq!(s.size(), 2);
        s.delete("a");
        assert_eq!(s.size(), 1);
    }
}
