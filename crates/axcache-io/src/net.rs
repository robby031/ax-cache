// crates/axcache-io/src/net.rs

use crate::buffer::IoBuffer;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use std::io::Result;

/// Wrapper untuk TcpListener yang khusus berjalan di dalam satu Core
pub struct CoreListener {
    listener: TcpListener,
}

impl CoreListener {
    pub fn bind(port: u16) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr)?;
        // Optimalisasi: Konfigurasi SO_REUSEPORT bisa ditambahkan di sini
        // untuk sistem produksi tinggi agar setiap core mendengar port yang sama.
        Ok(Self { listener })
    }

    /// Menerima koneksi baru. Tidak ada pemblokiran thread!
    #[inline(always)]
    pub async fn accept(&self) -> Result<(TcpStream, std::net::SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        // Menonaktifkan algoritma Nagle (NoDelay) untuk latensi mikrosekon
        stream.set_nodelay(true)?;
        Ok((stream, addr))
    }
}

/// Membaca stream menggunakan IoBuffer dengan model ownership (Zero-Copy kernel)
pub async fn read_stream(
    mut stream: TcpStream,
    buffer: IoBuffer,
) -> (Result<usize>, IoBuffer, TcpStream) {
    let raw_vec = buffer.into_inner();

    // Kernel mengambil alih raw_vec, melakukan I/O di background, dan mengembalikannya
    let (res, returned_vec) = stream.read(raw_vec).await;

    let reclaimed_buffer = IoBuffer::from_inner(returned_vec);
    (res, reclaimed_buffer, stream)
}

pub async fn write_stream(
    mut stream: TcpStream,
    data: Vec<u8>,
) -> (Result<usize>, Vec<u8>, TcpStream) {
    // Serahkan kepemilikan 'data' ke monoio/kernel.
    // write_all di monoio menjamin seluruh buffer akan dikirim secara utuh.
    let (res, returned_buf) = stream.write_all(data).await;

    // Kembalikan Result, buffer yang sudah tidak terpakai, dan stream yang aktif
    (res, returned_buf, stream)
}
