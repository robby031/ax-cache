// Network I/O layer menggunakan Monoio (io_uring / kqueue backend).
// SO_REUSEPORT memungkinkan semua worker mendengar pada port yang sama
// sehingga kernel mendistribusikan koneksi baru secara otomatis.

use crate::buffer::IoBuffer;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use std::io::Result;

/// TCP listener yang di-bind dengan SO_REUSEPORT agar bisa dishare lintas worker.
pub struct CoreListener {
    listener: TcpListener,
}

impl CoreListener {
    /// Bind port dengan SO_REUSEPORT.
    /// Setiap worker memanggil ini secara independen → kernel load-balance koneksi.
    pub fn bind_reuseport(port: u16) -> Result<Self> {
        let listener = create_reuseport_listener(port)?;
        Ok(Self { listener })
    }

    /// Fallback: bind port biasa (tanpa SO_REUSEPORT).
    pub fn bind(port: u16) -> Result<Self> {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr)?;
        Ok(Self { listener })
    }

    /// Accept koneksi baru secara async (completion-based, zero-syscall overhead).
    #[inline(always)]
    pub async fn accept(&self) -> Result<(TcpStream, std::net::SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        // TCP_NODELAY: matikan Nagle algorithm untuk latensi < 1ms
        stream.set_nodelay(true)?;
        Ok((stream, addr))
    }
}

/// Baca data dari stream menggunakan io_uring completion model.
/// Ownership buffer diserahkan ke kernel, dikembalikan setelah I/O selesai.
pub async fn read_stream(
    mut stream: TcpStream,
    buffer: IoBuffer,
) -> (Result<usize>, IoBuffer, TcpStream) {
    let raw_vec = buffer.into_inner();
    let (res, returned_vec) = stream.read(raw_vec).await;
    let reclaimed = IoBuffer::from_inner(returned_vec);
    (res, reclaimed, stream)
}

/// Tulis data ke stream. write_all menjamin semua bytes terkirim.
pub async fn write_stream(
    mut stream: TcpStream,
    data: Vec<u8>,
) -> (Result<usize>, Vec<u8>, TcpStream) {
    let (res, returned) = stream.write_all(data).await;
    (res, returned, stream)
}

// ============================================================================
// SO_REUSEPORT socket creation
// ============================================================================

fn create_reuseport_listener(port: u16) -> Result<TcpListener> {
    use std::net::SocketAddr;

    // Buat socket secara manual dengan socket2 untuk set SO_REUSEPORT
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;

    // SO_REUSEPORT: Kernel distributes incoming connections across all bound sockets
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_tcp_nodelay(true)?;

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    // Convert ke std::net::TcpListener lalu ke monoio::net::TcpListener
    let std_listener: std::net::TcpListener = socket.into();
    std_listener.set_nonblocking(true)?;

    TcpListener::from_std(std_listener)
}
