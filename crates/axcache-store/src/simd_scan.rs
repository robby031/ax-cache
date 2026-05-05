// crates/axcache-store/src/simd_scan.rs

use std::simd::{cmp::SimdPartialEq, u8x16};

/// Modul pencarian metadata menggunakan instruksi SIMD (Single Instruction, Multiple Data).
/// Mampu membandingkan 16 fingerprint secara paralel dalam 1 siklus CPU.

#[inline(always)]
pub fn match_fingerprint(metadata_group: &[u8; 16], target_fingerprint: u8) -> u16 {
    // 1. Muat 16 byte metadata dari array ke dalam register vektor CPU (SIMD)
    let meta_simd = u8x16::from_array(*metadata_group);

    // 2. Gandakan target fingerprint ke seluruh 16 jalur vektor (splat)
    // Contoh: [0xA1, 0xA1, 0xA1, ..., 0xA1]
    let target_simd = u8x16::splat(target_fingerprint);

    // 3. Bandingkan secara paralel. Hasilnya adalah mask (topeng) bit.
    let match_mask = meta_simd.simd_eq(target_simd);

    // 4. Ubah mask SIMD menjadi integer 16-bit standar.
    // Setiap bit '1' menandakan indeks di mana fingerprint cocok.
    match_mask.to_bitmask() as u16
}
