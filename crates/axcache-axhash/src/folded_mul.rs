// crates/axcache-axhash/src/folded_mul.rs

/// Konstanta prima berukuran besar yang dioptimalkan untuk arsitektur 64-bit.
/// Digunakan untuk memaksa distribusi bit secara acak (avalanche effect).
pub const PRIME_1: u64 = 0xA0761D6478BD642F;
pub const PRIME_2: u64 = 0xE7037ED1A0B428DB;

/// Implementasi fundamental matematis dari bit-folding (u64 x u64 -> u128 -> u64).
/// Teknik zero-cost abstraction yang memastikan pencampuran bit yang sempurna.
#[inline(always)]
pub const fn fold_mul(m1: u64, m2: u64) -> u64 {
    // 1. Kalikan dua u64 menjadi u128.
    // Sangat penting menggunakan `wrapping_mul` alih-alih `*` biasa untuk mencegah
    // terjadinya panic (integer overflow) saat aplikasi dijalankan di mode Debug.
    let result = (m1 as u128).wrapping_mul(m2 as u128);

    // 2. XOR-fold: Lipat 64-bit bagian atas (high) dengan 64-bit bagian bawah (low)
    // Pemisahan cast `as u64` secara eksplisit memastikan komputasi aman secara memori.
    ((result >> 64) as u64) ^ (result as u64)
}

/// Fungsi mixing lanjutan (State Mixer).
/// Mengombinasikan status internal (state) saat ini dengan data input yang baru masuk.
#[inline(always)]
pub const fn mix(state: u64, input: u64) -> u64 {
    // XOR input dengan state, lalu paksa pencampuran ekstrem menggunakan konstanta prima
    fold_mul(state ^ input, PRIME_1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fold_mul_basic() {
        let res = fold_mul(0x1234567890ABCDEF, 0xFEDCBA0987654321);
        assert_ne!(
            res, 0,
            "Hasil fold_mul tidak boleh 0 untuk kombinasi input non-trivial"
        );
    }

    #[test]
    fn test_fold_mul_commutative_property() {
        // Uji sifat komutatif: A * B harus menghasilkan nilai lipatan yang persis sama dengan B * A
        let a = 0x9E3779B97F4A7C15; // Golden ratio constant
        let b = 0xC15D5D1C9E3779B9;

        assert_eq!(
            fold_mul(a, b),
            fold_mul(b, a),
            "Kegagalan arsitektur: Operasi fold_mul harus bersifat komutatif"
        );
    }

    #[test]
    fn test_strict_avalanche_effect() {
        // Simulasi Avalanche: Jika kita membalikkan HANYA 1 bit dari input,
        // sekitar 50% bit dari output (sekitar 32 bit) harus ikut berubah.
        let base_input = 0x0000000000000000; // Mulai dari 0
        let hash_base = mix(base_input, PRIME_2);

        let mut total_flipped_bits = 0;

        // Kita geser bit 1 per 1 dari posisi 0 hingga 63
        for i in 0..64 {
            let modified_input = base_input ^ (1 << i);
            let hash_modified = mix(modified_input, PRIME_2);

            // Hitung jarak Hamming (jumlah bit yang berbeda antara hasil base dan modified)
            let flipped = (hash_base ^ hash_modified).count_ones();
            total_flipped_bits += flipped;
        }

        // Hitung rata-rata pergeseran bit
        let average_flipped = total_flipped_bits as f64 / 64.0;

        println!(
            "Rata-rata bit yang berubah (Avalanche): {:.2} dari 64 bit",
            average_flipped
        );

        // Kriteria Avalanche yang baik berada di rentang 28 hingga 36 bit yang berubah
        assert!(
            average_flipped > 28.0 && average_flipped < 36.0,
            "Distribusi Avalanche buruk: rata-rata hanya {} bit yang menyebar",
            average_flipped
        );
    }
}
