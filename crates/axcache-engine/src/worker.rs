// crates/axcache-engine/src/worker.rs

use crate::protocol::parse_command;
use axcache_store::shard::Shard;
use core_affinity::CoreId;
use std::cell::RefCell;
use std::rc::Rc;

// Integrasi abstraksi Zero-Copy dari modul axcache-io
use axcache_io::buffer::IoBuffer;
use axcache_io::net::{CoreListener, read_stream, write_stream};

/// Representasi satu Node Worker yang terisolasi
pub struct WorkerNode {
    pub core_id: usize,
    // Menggunakan Rc<RefCell> karena kita berada di arsitektur Thread-Per-Core murni.
    // Memungkinkan banyak task asinkron dalam 1 core mengakses Shard yang sama
    // TANPA overhead instruksi Mutex/RwLock yang biasa dipakai pada sistem multi-thread.
    pub local_shard: Rc<RefCell<Shard<String, Vec<u8>>>>,
}

impl WorkerNode {
    pub fn new(core_id: usize) -> Self {
        Self {
            core_id,
            local_shard: Rc::new(RefCell::new(Shard::new(core_id, 1024))),
        }
    }

    /// Event loop jaringan asinkron (Monoio)
    pub async fn run_event_loop(self, port: u16) {
        let listener = CoreListener::bind(port).expect("Gagal bind port jaringan");

        println!(
            "🔥 Worker pada Core {} siap menerima komando klien di port {}...",
            self.core_id, port
        );

        loop {
            if let Ok((stream, _client_addr)) = listener.accept().await {
                println!("🔌 Koneksi masuk dari: {}", _client_addr);

                let shard_ref = Rc::clone(&self.local_shard);

                monoio::spawn(async move {
                    // Simpan stream di variabel mutable agar ownership-nya bisa dipinjam-kembalikan
                    let mut active_stream = stream;

                    // LOOP KONEKSI PERSISTEN (Menangani banyak request dari 1 klien)
                    loop {
                        let mut buffer = IoBuffer::new(4096);

                        // 1. Baca data (Stream dipinjam ke kernel, lalu dikembalikan)
                        let (res, returned_buf, stream_back) =
                            read_stream(active_stream, buffer).await;

                        // Ambil kembali ownership stream
                        active_stream = stream_back;
                        buffer = returned_buf;

                        match res {
                            Ok(n) if n > 0 => {
                                let valid_data = buffer.as_slice(n);

                                match parse_command(valid_data) {
                                    Ok(cmd) => {
                                        // ==========================================
                                        // JEMBATAN OPERASI: Rute Komando ke Memori
                                        // ==========================================
                                        let response: Vec<u8> = {
                                            let mut shard = shard_ref.borrow_mut();

                                            match cmd {
                                                crate::protocol::ArchivedClientCommand::Get { key } => {
                                                    let key_str = key.as_str().to_string();
                                                    let core_id = shard.core_id;
                                                    if let Some(val) = shard.get(&key_str) {
                                                        println!("Core {}: GET Kunci -> '{}' (Ditemukan)", core_id, key_str);
                                                        let mut resp = b"+VALUE ".to_vec();
                                                        resp.extend_from_slice(val);
                                                        resp.extend_from_slice(b"\r\n");
                                                        resp
                                                    } else {
                                                        println!("Core {}: GET Kunci -> '{}' (Miss)", core_id, key_str);
                                                        b"-NOT FOUND\r\n".to_vec()
                                                    }
                                                }
                                                crate::protocol::ArchivedClientCommand::Set { key, value } => {
                                                    let key_str = key.as_str().to_string();
                                                    let val_vec = value.as_slice().to_vec();

                                                    shard.set(key_str.clone(), val_vec);
                                                    let core_id = shard.core_id;
                                                    println!("Core {}: SET Kunci -> '{}' sukses disimpan", core_id, key_str);

                                                    b"+OK\r\n".to_vec()
                                                }
                                                crate::protocol::ArchivedClientCommand::Delete { key } => {
                                                    let _key_str = key.as_str().to_string();
                                                    println!("Core {}: DELETE Kunci -> '{}'", shard.core_id, _key_str);
                                                    b"+DELETED\r\n".to_vec()
                                                }
                                            }
                                        }; // shard dilepas di sini

                                        // 2. Tulis balasan (Stream dipinjam ke kernel lagi, lalu dikembalikan)
                                        let (
                                            write_result,
                                            _recycled_response_buf,
                                            stream_back_again,
                                        ) = write_stream(active_stream, response).await;

                                        // Ambil kembali ownership stream untuk putaran loop berikutnya
                                        active_stream = stream_back_again;

                                        if let Err(e) = write_result {
                                            eprintln!(
                                                "❌ Gagal membalas klien via io_uring: {:?}",
                                                e
                                            );
                                            break; // Keluar dari loop klien jika gagal menulis
                                        }
                                    }
                                    Err(e) => {
                                        let error_msg =
                                            format!("-ERR RKYV Parse failed: {:?}\r\n", e);
                                        let (_, _, stream_error) =
                                            write_stream(active_stream, error_msg.into_bytes())
                                                .await;
                                        active_stream = stream_error;
                                    }
                                }
                            }
                            _ => {
                                // Jika res == Ok(0), berarti klien secara rahasia menutup koneksi (EOF).
                                // Jika Err, berarti terjadi gangguan koneksi jaringan.
                                println!("🔌 Klien terputus atau selesai.");
                                break; // Keluar dari loop untuk menutup stream dengan aman
                            }
                        }
                    }
                });
            }
        }
    }
}

pub fn spawn_pinned_worker(logical_core_id: usize, port: u16) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let core_ids = core_affinity::get_core_ids().expect("Gagal membaca topologi CPU");

        if logical_core_id < core_ids.len() {
            let core: CoreId = core_ids[logical_core_id];

            core_affinity::set_for_current(core);
            println!("📌 Thread worker di-pin ke CPU Core {}", core.id);

            let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .with_entries(1024)
                .build()
                .expect("Gagal inisialisasi monoio runtime");

            let worker = WorkerNode::new(logical_core_id);
            rt.block_on(worker.run_event_loop(port));
        } else {
            eprintln!("Core ID {} di luar batas.", logical_core_id);
        }
    })
}
