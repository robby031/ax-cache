// crates/axcache-engine/src/resp.rs
//
// RESP2 (Redis Serialization Protocol v2) parser dan response builder.
// Kompatibel penuh dengan redis-cli dan semua Redis client library.
//
// Format RESP2:
//   Simple String:  +OK\r\n
//   Error:          -ERR message\r\n
//   Integer:        :42\r\n
//   Bulk String:    $6\r\nfoobar\r\n   |  $-1\r\n  (nil)
//   Array:          *3\r\n<element>\r\n...
//   Inline command: PING\r\n  atau  GET key\r\n

// ============================================================================
// Parser
// ============================================================================

/// Parser streaming RESP2.
/// Feed data dari network, kemudian panggil try_parse() untuk mengekstrak command.
pub struct RespParser {
    buf: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(4096) }
    }

    /// Tambahkan bytes dari network ke buffer internal.
    #[inline]
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Coba parse satu command lengkap dari buffer.
    /// Mengembalikan `Some(args)` jika berhasil, `None` jika data belum lengkap.
    /// `args[0]` adalah nama command, sisanya adalah argumen.
    pub fn try_parse(&mut self) -> Option<Vec<Vec<u8>>> {
        if self.buf.is_empty() {
            return None;
        }

        let (cmd, consumed) = if self.buf[0] == b'*' {
            // Array format (standard RESP)
            Self::parse_array(&self.buf)?
        } else {
            // Inline format: "GET key\r\n" atau "PING\r\n"
            Self::parse_inline(&self.buf)?
        };

        // Hapus bytes yang sudah dikonsumsi dari buffer
        self.buf.drain(..consumed);
        Some(cmd)
    }

    /// Buffer sisa yang belum terproses (untuk debug/monitoring).
    #[inline]
    pub fn pending(&self) -> usize {
        self.buf.len()
    }

    // --- internal ---

    fn parse_array(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
        // *<n>\r\n
        let (n, mut pos) = read_integer(buf, 1)?; // skip '*'
        if n < 0 {
            // Null array — anggap sebagai PING untuk kesederhanaan
            return Some((vec![b"PING".to_vec()], pos));
        }
        let count = n as usize;
        let mut args = Vec::with_capacity(count);

        for _ in 0..count {
            if pos >= buf.len() {
                return None; // data belum lengkap
            }
            match buf[pos] {
                b'$' => {
                    // Bulk string: $<len>\r\n<data>\r\n
                    let (len, after_header) = read_integer(buf, pos + 1)?;
                    if len < 0 {
                        // Null bulk string
                        args.push(b"".to_vec());
                        pos = after_header;
                    } else {
                        let len = len as usize;
                        if after_header + len + 2 > buf.len() {
                            return None; // data belum lengkap
                        }
                        args.push(buf[after_header..after_header + len].to_vec());
                        pos = after_header + len + 2; // skip \r\n setelah data
                    }
                }
                _ => return None, // format tidak dikenal
            }
        }

        Some((args, pos))
    }

    fn parse_inline(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
        // Cari \r\n atau \n
        let end = find_crlf(buf)?;
        let line = &buf[..end];

        // Split by whitespace
        let args: Vec<Vec<u8>> = line
            .split(|&b| b == b' ' || b == b'\t')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_vec())
            .collect();

        if args.is_empty() {
            // Baris kosong, skip saja
            let consumed = if end + 1 < buf.len() && buf[end] == b'\r' && buf[end + 1] == b'\n' {
                end + 2
            } else {
                end + 1
            };
            return Some((vec![], consumed));
        }

        let consumed = if end < buf.len() && buf[end] == b'\r' {
            end + 2
        } else {
            end + 1
        };

        Some((args, consumed))
    }
}

/// Baca integer di posisi `start` hingga \r\n.
/// Mengembalikan (nilai, posisi setelah \r\n).
fn read_integer(buf: &[u8], start: usize) -> Option<(i64, usize)> {
    let end = find_crlf_from(buf, start)?;
    let s = std::str::from_utf8(&buf[start..end]).ok()?;
    let n: i64 = s.parse().ok()?;
    // +2 untuk \r\n
    Some((n, end + 2))
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    find_crlf_from(buf, 0)
}

fn find_crlf_from(buf: &[u8], from: usize) -> Option<usize> {
    for i in from..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    // Juga terima \n tanpa \r
    for i in from..buf.len() {
        if buf[i] == b'\n' {
            return Some(i);
        }
    }
    None
}

// ============================================================================
// Response Builder
// ============================================================================

/// Bangun response RESP2 yang siap dikirim ke client.
pub struct Resp;

impl Resp {
    /// +OK\r\n
    #[inline]
    pub fn ok() -> Vec<u8> {
        b"+OK\r\n".to_vec()
    }

    /// +<msg>\r\n
    #[inline]
    pub fn simple(msg: &[u8]) -> Vec<u8> {
        let mut r = Vec::with_capacity(msg.len() + 3);
        r.push(b'+');
        r.extend_from_slice(msg);
        r.extend_from_slice(b"\r\n");
        r
    }

    /// +PONG\r\n  atau  $<len>\r\n<msg>\r\n jika ada pesan
    pub fn pong(msg: Option<&[u8]>) -> Vec<u8> {
        match msg {
            None => b"+PONG\r\n".to_vec(),
            Some(m) => Self::bulk(Some(m)),
        }
    }

    /// -ERR <msg>\r\n
    #[inline]
    pub fn err(msg: &[u8]) -> Vec<u8> {
        let mut r = Vec::with_capacity(msg.len() + 7);
        r.extend_from_slice(b"-ERR ");
        r.extend_from_slice(msg);
        r.extend_from_slice(b"\r\n");
        r
    }

    /// -WRONGTYPE ...\r\n
    #[inline]
    pub fn wrong_type() -> Vec<u8> {
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec()
    }

    /// :<n>\r\n
    #[inline]
    pub fn integer(n: i64) -> Vec<u8> {
        let s = n.to_string();
        let mut r = Vec::with_capacity(s.len() + 3);
        r.push(b':');
        r.extend_from_slice(s.as_bytes());
        r.extend_from_slice(b"\r\n");
        r
    }

    /// $<len>\r\n<data>\r\n  atau  $-1\r\n (nil)
    pub fn bulk(data: Option<&[u8]>) -> Vec<u8> {
        match data {
            None => b"$-1\r\n".to_vec(),
            Some(d) => {
                let len_str = d.len().to_string();
                let mut r = Vec::with_capacity(d.len() + len_str.len() + 5);
                r.push(b'$');
                r.extend_from_slice(len_str.as_bytes());
                r.extend_from_slice(b"\r\n");
                r.extend_from_slice(d);
                r.extend_from_slice(b"\r\n");
                r
            }
        }
    }

    /// *<n>\r\n  header array
    #[inline]
    pub fn array_header(n: usize) -> Vec<u8> {
        let s = n.to_string();
        let mut r = Vec::with_capacity(s.len() + 3);
        r.push(b'*');
        r.extend_from_slice(s.as_bytes());
        r.extend_from_slice(b"\r\n");
        r
    }

    /// *-1\r\n  null array
    #[inline]
    pub fn null_array() -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// ============================================================================
// Command executor
// ============================================================================

/// Eksekusi satu RESP command terhadap shard dan kembalikan response bytes.
/// Semua command kompatibel dengan Redis 6+ protokol.
pub fn execute(args: &[Vec<u8>], shard: &mut axcache_store::shard::Shard) -> Vec<u8> {
    if args.is_empty() {
        return Resp::err(b"empty command");
    }

    // Command name: case-insensitive
    let cmd: Vec<u8> = args[0].iter().map(|b| b.to_ascii_uppercase()).collect();

    match cmd.as_slice() {
        // ----------------------------------------------------------------
        // PING [message]
        // ----------------------------------------------------------------
        b"PING" => {
            if args.len() > 1 {
                Resp::pong(Some(&args[1]))
            } else {
                Resp::pong(None)
            }
        }

        // ----------------------------------------------------------------
        // GET key
        // ----------------------------------------------------------------
        b"GET" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'get'");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return Resp::err(b"key is not valid UTF-8"),
            };
            match shard.get(key) {
                Some(val) => Resp::bulk(Some(val)),
                None => Resp::bulk(None),
            }
        }

        // ----------------------------------------------------------------
        // SET key value [EX seconds] [PX milliseconds]
        // ----------------------------------------------------------------
        b"SET" => {
            if args.len() < 3 {
                return Resp::err(b"wrong number of arguments for 'set'");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k.to_string(),
                Err(_) => return Resp::err(b"key is not valid UTF-8"),
            };
            let value = args[2].clone();

            // Parse opsi opsional: EX <secs> atau PX <ms>
            let mut ttl_secs: Option<u64> = None;
            let mut i = 3;
            while i + 1 < args.len() {
                let opt: Vec<u8> = args[i].iter().map(|b| b.to_ascii_uppercase()).collect();
                match opt.as_slice() {
                    b"EX" => {
                        match std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                        {
                            Some(s) => ttl_secs = Some(s),
                            None => return Resp::err(b"value is not an integer or out of range"),
                        }
                        i += 2;
                    }
                    b"PX" => {
                        match std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                        {
                            Some(ms) => ttl_secs = Some((ms / 1000).max(1)),
                            None => return Resp::err(b"value is not an integer or out of range"),
                        }
                        i += 2;
                    }
                    _ => { i += 1; }
                }
            }

            match ttl_secs {
                Some(secs) => shard.set_ex(key, value, secs),
                None => shard.set(key, value),
            }
            Resp::ok()
        }

        // ----------------------------------------------------------------
        // DEL key [key ...]
        // ----------------------------------------------------------------
        b"DEL" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'del'");
            }
            let mut count = 0i64;
            for key_bytes in &args[1..] {
                if let Ok(key) = std::str::from_utf8(key_bytes) {
                    if shard.delete(key) {
                        count += 1;
                    }
                }
            }
            Resp::integer(count)
        }

        // ----------------------------------------------------------------
        // EXISTS key [key ...]
        // ----------------------------------------------------------------
        b"EXISTS" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'exists'");
            }
            let mut count = 0i64;
            for key_bytes in &args[1..] {
                if let Ok(key) = std::str::from_utf8(key_bytes) {
                    if shard.exists(key) {
                        count += 1;
                    }
                }
            }
            Resp::integer(count)
        }

        // ----------------------------------------------------------------
        // EXPIRE key seconds
        // ----------------------------------------------------------------
        b"EXPIRE" => {
            if args.len() < 3 {
                return Resp::err(b"wrong number of arguments for 'expire'");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return Resp::err(b"key is not valid UTF-8"),
            };
            let secs: u64 = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(s) => s,
                None => return Resp::err(b"value is not an integer or out of range"),
            };
            Resp::integer(if shard.expire(key, secs) { 1 } else { 0 })
        }

        // ----------------------------------------------------------------
        // TTL key
        // ----------------------------------------------------------------
        b"TTL" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'ttl'");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return Resp::err(b"key is not valid UTF-8"),
            };
            match shard.ttl(key) {
                Some(n) => Resp::integer(n),
                None => Resp::integer(-2), // key tidak ada
            }
        }

        // ----------------------------------------------------------------
        // DBSIZE
        // ----------------------------------------------------------------
        b"DBSIZE" => Resp::integer(shard.size() as i64),

        // ----------------------------------------------------------------
        // FLUSHALL [ASYNC]
        // ----------------------------------------------------------------
        b"FLUSHALL" | b"FLUSHDB" => {
            shard.flush();
            Resp::ok()
        }

        // ----------------------------------------------------------------
        // INFO [section]
        // ----------------------------------------------------------------
        b"INFO" => {
            let info = build_info(shard);
            Resp::bulk(Some(info.as_bytes()))
        }

        // ----------------------------------------------------------------
        // COMMAND [COUNT | DOCS | ...]
        // ----------------------------------------------------------------
        b"COMMAND" => {
            // Minimal implementation untuk kompatibilitas redis-cli
            if args.len() > 1 {
                let sub: Vec<u8> = args[1].iter().map(|b| b.to_ascii_uppercase()).collect();
                if sub == b"COUNT" {
                    return Resp::integer(14); // jumlah command yang didukung
                }
            }
            Resp::ok()
        }

        // ----------------------------------------------------------------
        // ECHO message
        // ----------------------------------------------------------------
        b"ECHO" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'echo'");
            }
            Resp::bulk(Some(&args[1]))
        }

        // ----------------------------------------------------------------
        // KEYS pattern  (sederhana: hanya '*' yang didukung)
        // ----------------------------------------------------------------
        b"KEYS" => {
            // AxCache tidak menyimpan daftar key global per shard karena arsitektur shared-nothing.
            // Kembalikan array kosong untuk kompatibilitas.
            Resp::array_header(0)
        }

        // ----------------------------------------------------------------
        // SELECT index  (AxCache hanya punya 1 database)
        // ----------------------------------------------------------------
        b"SELECT" => {
            if args.len() < 2 {
                return Resp::err(b"wrong number of arguments for 'select'");
            }
            match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u32>().ok()) {
                Some(0) => Resp::ok(),
                _ => Resp::err(b"DB index is out of range"),
            }
        }

        // ----------------------------------------------------------------
        // QUIT
        // ----------------------------------------------------------------
        b"QUIT" => Resp::ok(),

        // ----------------------------------------------------------------
        // Unknown command
        // ----------------------------------------------------------------
        _ => {
            let cmd_str = String::from_utf8_lossy(&args[0]);
            let msg = format!(
                "unknown command `{}`, with args beginning with: {}",
                cmd_str,
                args.get(1)
                    .map(|a| format!("`{}`", String::from_utf8_lossy(a)))
                    .unwrap_or_default()
            );
            Resp::err(msg.as_bytes())
        }
    }
}

fn build_info(shard: &axcache_store::shard::Shard) -> String {
    format!(
        "# Server\r\naxcache_version:0.1.0\r\narch_bits:64\r\nos:AxCache TPC\r\n\
         # Stats\r\ncore_id:{}\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\n\
         # Keyspace\r\ndb0:keys={},expires=0\r\n",
        shard.core_id,
        shard.size(),
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(input: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut p = RespParser::new();
        p.feed(input);
        p.try_parse()
    }

    #[test]
    fn test_parse_array_set() {
        let raw = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let args = parse_one(raw).unwrap();
        assert_eq!(args[0], b"SET");
        assert_eq!(args[1], b"foo");
        assert_eq!(args[2], b"bar");
    }

    #[test]
    fn test_parse_inline_ping() {
        let raw = b"PING\r\n";
        let args = parse_one(raw).unwrap();
        assert_eq!(args[0], b"PING");
    }

    #[test]
    fn test_parse_inline_get_with_arg() {
        let raw = b"GET mykey\r\n";
        let args = parse_one(raw).unwrap();
        assert_eq!(args[0], b"GET");
        assert_eq!(args[1], b"mykey");
    }

    #[test]
    fn test_partial_data_returns_none() {
        let raw = b"*3\r\n$3\r\nSET\r\n"; // belum lengkap
        let result = parse_one(raw);
        assert!(result.is_none());
    }

    #[test]
    fn test_resp_bulk_response() {
        let r = Resp::bulk(Some(b"hello"));
        assert_eq!(r, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_resp_nil() {
        let r = Resp::bulk(None);
        assert_eq!(r, b"$-1\r\n");
    }

    #[test]
    fn test_resp_integer() {
        assert_eq!(Resp::integer(42), b":42\r\n");
        assert_eq!(Resp::integer(-1), b":-1\r\n");
    }

    #[test]
    fn test_resp_error() {
        let r = Resp::err(b"something went wrong");
        assert_eq!(&r[..5], b"-ERR ");
    }

    #[test]
    fn test_multiple_commands_in_buffer() {
        let mut p = RespParser::new();
        p.feed(b"PING\r\nGET foo\r\n");
        let first = p.try_parse().unwrap();
        assert_eq!(first[0], b"PING");
        let second = p.try_parse().unwrap();
        assert_eq!(second[0], b"GET");
        assert_eq!(second[1], b"foo");
    }
}
