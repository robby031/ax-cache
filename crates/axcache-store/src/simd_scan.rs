// crates/axcache-store/src/simd_scan.rs
//
// SIMD metadata scanning untuk DashTable — 16 fingerprint dibandingkan dalam 1 siklus CPU.
// Menggunakan portable_simd (core::simd) agar berjalan di x86, ARM64, dan RISC-V.

use std::simd::{cmp::SimdPartialEq, cmp::SimdPartialOrd, u8x16};

/// EMPTY = slot belum pernah digunakan.
pub const EMPTY: u8 = 0x00;

/// DELETED = slot pernah dipakai tapi sudah dihapus (tombstone).
/// Probing HARUS tetap melanjutkan melewati tombstone.
pub const DELETED: u8 = 0x01;

/// Fingerprint slot aktif selalu berada di range 0x80–0xFF (bit MSB selalu 1).
/// Ini menjamin EMPTY dan DELETED tidak pernah bertubrukan dengan fingerprint.
#[inline(always)]
pub fn make_fingerprint(hash: u64) -> u8 {
    // Ambil 7 bit bawah hash, set MSB agar selalu ≥ 0x80
    (hash & 0x7F | 0x80) as u8
}

/// Bandingkan 16 fingerprint secara paralel dalam 1 siklus CPU.
/// Mengembalikan bitmask 16-bit: bit ke-i = 1 jika fingerprints[i] == target.
#[inline(always)]
pub fn match_fingerprint(fingerprints: &[u8; 16], target: u8) -> u16 {
    let meta = u8x16::from_array(*fingerprints);
    let tgt = u8x16::splat(target);
    meta.simd_eq(tgt).to_bitmask() as u16
}

/// Temukan slot yang KOSONG (EMPTY=0x00) atau TOMBSTONE (DELETED=0x01) untuk insert.
/// Bit ke-i = 1 jika slot ke-i tersedia (belum terisi atau sudah dihapus).
/// Fingerprint aktif selalu ≥ 0x80, jadi "tersedia" ≡ nilai < 0x80.
#[inline(always)]
pub fn find_available(fingerprints: &[u8; 16]) -> u16 {
    let meta = u8x16::from_array(*fingerprints);
    // Slot aktif ≥ 0x80, slot tersedia < 0x80
    let threshold = u8x16::splat(0x80);
    meta.simd_lt(threshold).to_bitmask() as u16
}

/// Cek apakah SELURUH 16 slot dalam grup adalah EMPTY (0x00).
/// Digunakan sebagai kondisi terminasi probing: jika grup ini sepenuhnya kosong,
/// key yang dicari pasti tidak ada di grup manapun lebih jauh.
#[inline(always)]
pub fn all_empty(fingerprints: &[u8; 16]) -> bool {
    let meta = u8x16::from_array(*fingerprints);
    let zero = u8x16::splat(EMPTY);
    // Semua sama dengan EMPTY ↔ bitmask = 0xFFFF
    meta.simd_eq(zero).to_bitmask() == 0xFFFF
}

/// Temukan slot EMPTY saja (bukan DELETED) — digunakan di `resize()`.
#[inline(always)]
pub fn find_empty(fingerprints: &[u8; 16]) -> u16 {
    let meta = u8x16::from_array(*fingerprints);
    meta.simd_eq(u8x16::splat(EMPTY)).to_bitmask() as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_fingerprint() {
        let fps: [u8; 16] = [
            0x80, 0x00, 0xAB, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        // Match 0x80: bit 0 dan 3 harus 1
        let mask = match_fingerprint(&fps, 0x80);
        assert_eq!(mask & 1, 1, "bit 0 harus match");
        assert_eq!((mask >> 3) & 1, 1, "bit 3 harus match");
        assert_eq!((mask >> 2) & 1, 0, "bit 2 tidak boleh match");
    }

    #[test]
    fn test_all_empty_true() {
        let fps = [EMPTY; 16];
        assert!(all_empty(&fps));
    }

    #[test]
    fn test_all_empty_false_with_deleted() {
        let mut fps = [EMPTY; 16];
        fps[5] = DELETED;
        assert!(!all_empty(&fps), "DELETED bukan EMPTY");
    }

    #[test]
    fn test_find_available() {
        let mut fps = [0x80u8; 16]; // semua occupied
        fps[3] = EMPTY;
        fps[7] = DELETED;
        let mask = find_available(&fps);
        assert_ne!(mask & (1 << 3), 0, "slot 3 (EMPTY) harus available");
        assert_ne!(mask & (1 << 7), 0, "slot 7 (DELETED) harus available");
        assert_eq!(mask & 1, 0, "slot 0 (occupied) tidak available");
    }

    #[test]
    fn test_make_fingerprint_always_gte_0x80() {
        for hash in [0u64, 1, 0x7F, 0xFF, 0xDEAD_BEEF, u64::MAX] {
            let fp = make_fingerprint(hash);
            assert!(fp >= 0x80, "fingerprint harus ≥ 0x80, got {:#x}", fp);
        }
    }
}
