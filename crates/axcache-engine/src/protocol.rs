// crates/axcache-engine/src/protocol.rs

use rkyv::{Archive, Deserialize, Serialize, rancor::Error};

/// Definisi perintah klien menggunakan format rkyv terbaru.
/// Makro baru memungkinkan pewarisan sifat (derive) langsung ke ArchivedClientCommand.
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // Membangkitkan implementasi PartialEq antara tipe asli dan tipe archived
    compare(PartialEq),
    // Menurunkan trait Debug ke tipe archived
    derive(Debug),
)]
pub enum ClientCommand {
    /// Mengambil nilai berdasarkan kunci
    Get { key: String },

    /// Menyimpan kunci dan nilai (dalam bentuk raw bytes)
    Set { key: String, value: Vec<u8> },

    /// Perintah spesifik untuk menghapus data
    Delete { key: String },
}

/// Helper untuk mengakses perintah dari byte array (Zero-Copy Deserialization)
#[inline]
pub fn parse_command(buffer: &[u8]) -> Result<&ArchivedClientCommand, Error> {
    // rkyv::access adalah API aman (safe API) terbaru yang akan memvalidasi byte
    // menggunakan `bytecheck` sebelum mengizinkan akses ke data.
    // Tidak ada alokasi heap yang terjadi di sini!
    rkyv::access::<ArchivedClientCommand, Error>(buffer)
}

/// Helper opsional jika AxCache perlu mengirim respons balik dalam format rkyv
pub fn serialize_command(command: &ClientCommand) -> Result<Vec<u8>, Error> {
    // Serialisasi sederhana dengan satu panggilan fungsi
    let bytes = rkyv::to_bytes::<Error>(command)?;
    Ok(bytes.into_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_parsing() {
        let cmd = ClientCommand::Set {
            key: "session_123".to_string(),
            value: vec![1, 2, 3, 4],
        };

        // 1. Klien mengirim byte ke server
        let bytes = serialize_command(&cmd).expect("Gagal serialize");

        // 2. Server AxCache menerima byte dari jaringan via io_uring
        // 3. Server langsung membaca (zero-copy) tanpa deserialize!
        let archived = parse_command(&bytes).expect("Gagal validasi zero-copy");

        // Karena kita menggunakan #[rkyv(compare(PartialEq))], kita bisa langsung
        // membandingkan tipe Archived dengan tipe asli:
        assert_eq!(archived, &cmd);
    }
}
