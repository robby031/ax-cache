use monoio::fs::File;
use std::io::Result;

/// Logika Fork-less disk snapshotting asinkron.
/// Dipanggil secara periodik oleh setiap worker tanpa menghentikan request klien.
pub async fn save_shard_to_disk(core_id: usize, archived_data: Vec<u8>) -> Result<()> {
    let filename = format!("axcache_snapshot_core_{}.rdb", core_id);

    // Buka file secara asinkron.
    // Perhatikan: Tidak perlu 'mut' karena io_uring menggunakan positional I/O!
    let file = File::create(&filename).await?;

    // Tulis ke disk menggunakan backend io_uring / kqueue.
    // Menggunakan `write_all_at` dengan offset 0 karena ini file baru.
    // Ownership data dilempar ke kernel dan ditangkap kembali (Sistem Rent).
    let (res, _returned_buf) = file.write_all_at(archived_data, 0).await;
    res?;

    // Flush metadata ke disk untuk menjamin durabilitas (persistence)
    file.sync_all().await?;

    println!(
        "Core {}: Snapshot berhasil disimpan ke disk tanpa forking!",
        core_id
    );
    Ok(())
}
