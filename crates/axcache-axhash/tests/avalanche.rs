// crates/axcache-axhash/tests/avalanche.rs

use axcache_axhash::hash_item;

#[test]
fn test_avalanche_effect() {
    let input1 = b"hello axhash1";
    let input2 = b"hello axhash2"; // Hanya beda 1 karakter di akhir

    let hash1 = hash_item(&input1);
    let hash2 = hash_item(&input2);

    // Hitung jumlah bit yang berbeda (Hamming Distance)
    let bit_diff = (hash1 ^ hash2).count_ones();

    // Hash u64 memiliki 64 bit. Avalanche effect yang baik akan mengubah sekitar 32 bit (~50%)
    println!("Hash 1: {:016x}", hash1);
    println!("Hash 2: {:016x}", hash2);
    println!("Bit berbeda (Hamming Distance): {} dari 64", bit_diff);

    // Pastikan perbedaannya berada dalam margin yang wajar (misal antara 20 hingga 44 bit)
    assert!(
        bit_diff > 20 && bit_diff < 44,
        "Distribusi bit tidak memenuhi Avalanche Effect!"
    );
}
