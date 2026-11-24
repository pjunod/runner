//! Just enough RAR to find a stored file's bytes in the first volume.
//!
//! Scene releases wrap the media in RAR volumes that are almost always
//! store-mode (method "store": the bytes are the file, uncompressed). For
//! those, the media file's first bytes sit right after the file header of
//! the first volume, and no decompression is needed to read them. Anything
//! else — compressed, encrypted, or a volume that is not the first — is
//! reported as such, and the caller says why it could not peek.
//!
//! Formats: RAR 1.5–4.x ("RAR4", signature `Rar!\x1a\x07\x00`) and RAR 5.0
//! (signature `Rar!\x1a\x07\x01\x00`). Pure and bounded: it reads only the
//! bytes it is given and never panics on arbitrary input.

/// What the head of a volume says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Volume {
    /// Not a RAR archive at all.
    NotRar,
    /// The first volume, holding a stored file whose data starts at
    /// `data_offset` in this volume and runs `packed` bytes in it.
    Stored {
        name: String,
        unpacked: u64,
        data_offset: u64,
        packed: u64,
    },
    /// A later volume of a set: the member's data continues from an earlier one.
    NotFirst,
    /// Password-protected headers or file data.
    Encrypted,
    /// The member is compressed (RAR method other than store).
    Compressed,
    /// The headers did not fit in the bytes given, or made no sense.
    Unreadable(&'static str),
}

const RAR4_SIG: &[u8] = b"Rar!\x1a\x07\x00";
const RAR5_SIG: &[u8] = b"Rar!\x1a\x07\x01\x00";

/// Inspect the head of a volume.
pub fn inspect(b: &[u8]) -> Volume {
    if b.starts_with(RAR5_SIG) {
        rar5(b)
    } else if b.starts_with(RAR4_SIG) {
        rar4(b)
    } else {
        Volume::NotRar
    }
}

fn u16le(b: &[u8], at: usize) -> Option<u64> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?) as u64)
}

fn u32le(b: &[u8], at: usize) -> Option<u64> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?) as u64)
}

// RAR4 block types and flags (unrar's technote).
const R4_MAIN: u8 = 0x73;
const R4_FILE: u8 = 0x74;
const R4_END: u8 = 0x7b;
const R4_LONG_BLOCK: u64 = 0x8000;
const R4_MAIN_PASSWORD: u64 = 0x0080; // block headers are encrypted
const R4_FILE_SPLIT_BEFORE: u64 = 0x0001;
const R4_FILE_PASSWORD: u64 = 0x0004;
const R4_FILE_DIRECTORY: u64 = 0x00e0;
const R4_FILE_LARGE: u64 = 0x0100;
const R4_METHOD_STORE: u8 = 0x30;

fn rar4(b: &[u8]) -> Volume {
    let mut at = RAR4_SIG.len();
    for _ in 0..64 {
        // HEAD_CRC(2) HEAD_TYPE(1) HEAD_FLAGS(2) HEAD_SIZE(2) [ADD_SIZE(4)]
        let (Some(&kind), Some(flags), Some(size)) =
            (b.get(at + 2), u16le(b, at + 3), u16le(b, at + 5))
        else {
            return Volume::Unreadable("RAR4 header runs past the bytes read");
        };
        if size < 7 {
            return Volume::Unreadable("RAR4 header too short");
        }
        let add = if flags & R4_LONG_BLOCK != 0 {
            match u32le(b, at + 7) {
                Some(n) => n,
                None => return Volume::Unreadable("RAR4 header runs past the bytes read"),
            }
        } else {
            0
        };
        match kind {
            R4_MAIN if flags & R4_MAIN_PASSWORD != 0 => return Volume::Encrypted,
            R4_FILE => {
                // The whole header must be in hand, as for RAR5: the caller
                // reads further and asks again.
                if at + size as usize > b.len() {
                    return Volume::Unreadable("RAR4 file header runs past the bytes read");
                }
                // PACK_SIZE(4) UNP_SIZE(4) HOST_OS(1) FILE_CRC(4) FTIME(4)
                // UNP_VER(1) METHOD(1) NAME_SIZE(2) ATTR(4) [HIGH_PACK(4) HIGH_UNP(4)] NAME
                let h = at + 7;
                let (Some(pack_lo), Some(unp_lo), Some(&method), Some(name_len)) = (
                    u32le(b, h),
                    u32le(b, h + 4),
                    b.get(h + 18),
                    u16le(b, h + 19),
                ) else {
                    return Volume::Unreadable("RAR4 file header runs past the bytes read");
                };
                let mut name_at = h + 25;
                let (mut packed, mut unpacked) = (pack_lo, unp_lo);
                if flags & R4_FILE_LARGE != 0 {
                    let (Some(hp), Some(hu)) = (u32le(b, h + 25), u32le(b, h + 29)) else {
                        return Volume::Unreadable("RAR4 file header runs past the bytes read");
                    };
                    packed |= hp << 32;
                    unpacked |= hu << 32;
                    name_at += 8;
                }
                if flags & R4_FILE_DIRECTORY == R4_FILE_DIRECTORY {
                    at += size as usize + add as usize;
                    continue;
                }
                if flags & R4_FILE_SPLIT_BEFORE != 0 {
                    return Volume::NotFirst;
                }
                if flags & R4_FILE_PASSWORD != 0 {
                    return Volume::Encrypted;
                }
                if method != R4_METHOD_STORE {
                    return Volume::Compressed;
                }
                let name = b
                    .get(name_at..name_at + name_len as usize)
                    .map(|n| {
                        String::from_utf8_lossy(n.split(|&c| c == 0).next().unwrap_or(n))
                            .into_owned()
                    })
                    .unwrap_or_default();
                return Volume::Stored {
                    name,
                    unpacked,
                    data_offset: (at + size as usize) as u64,
                    packed,
                };
            }
            R4_END => return Volume::Unreadable("RAR4 archive ended before a file"),
            _ => {}
        }
        at = match at
            .checked_add(size as usize)
            .and_then(|n| n.checked_add(add as usize))
        {
            Some(n) => n,
            None => return Volume::Unreadable("RAR4 block size overflows"),
        };
    }
    Volume::Unreadable("RAR4: no file header in the first 64 blocks")
}

/// RAR5 variable-length integer: 7 bits per byte, high bit = more.
fn vint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*at)?;
        *at += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

// RAR5 header types and flags (rarlab's RAR 5.0 archive format).
const R5_MAIN: u64 = 1;
const R5_FILE: u64 = 2;
const R5_ENCRYPTION: u64 = 4;
const R5_END: u64 = 5;
const R5_HAS_EXTRA: u64 = 0x0001;
const R5_HAS_DATA: u64 = 0x0002;
const R5_SPLIT_BEFORE: u64 = 0x0008;
const R5_FILE_DIRECTORY: u64 = 0x0001;
const R5_FILE_MTIME: u64 = 0x0002;
const R5_FILE_CRC: u64 = 0x0004;
const R5_EXTRA_ENCRYPTION: u64 = 0x01;

fn rar5(b: &[u8]) -> Volume {
    let mut at = RAR5_SIG.len();
    for _ in 0..64 {
        // CRC32(4) HEADER_SIZE(vint) then HEADER_SIZE bytes, then the data.
        let mut p = at + 4;
        let Some(header_size) = vint(b, &mut p) else {
            return Volume::Unreadable("RAR5 header runs past the bytes read");
        };
        let header_start = p;
        let Some(header_end) = header_start.checked_add(header_size as usize) else {
            return Volume::Unreadable("RAR5 header size overflows");
        };
        if header_end > b.len() {
            return Volume::Unreadable("RAR5 header runs past the bytes read");
        }
        let h = &b[..header_end];
        let (Some(kind), Some(flags)) = (vint(h, &mut p), vint(h, &mut p)) else {
            return Volume::Unreadable("RAR5 header is malformed");
        };
        let extra = if flags & R5_HAS_EXTRA != 0 {
            vint(h, &mut p)
        } else {
            Some(0)
        };
        let data = if flags & R5_HAS_DATA != 0 {
            vint(h, &mut p)
        } else {
            Some(0)
        };
        let (Some(extra), Some(data)) = (extra, data) else {
            return Volume::Unreadable("RAR5 header is malformed");
        };
        match kind {
            R5_ENCRYPTION => return Volume::Encrypted,
            R5_FILE => {
                let file_flags = vint(h, &mut p);
                let unpacked = vint(h, &mut p);
                let _attrs = vint(h, &mut p);
                let (Some(file_flags), Some(unpacked)) = (file_flags, unpacked) else {
                    return Volume::Unreadable("RAR5 file header is malformed");
                };
                if file_flags & R5_FILE_MTIME != 0 {
                    p += 4;
                }
                if file_flags & R5_FILE_CRC != 0 {
                    p += 4;
                }
                let compression = vint(h, &mut p);
                let _host = vint(h, &mut p);
                let name_len = vint(h, &mut p);
                let (Some(compression), Some(name_len)) = (compression, name_len) else {
                    return Volume::Unreadable("RAR5 file header is malformed");
                };
                let name = h
                    .get(p..p.saturating_add(name_len as usize))
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .unwrap_or_default();
                if file_flags & R5_FILE_DIRECTORY != 0 {
                    at = match header_end.checked_add(data as usize) {
                        Some(n) => n,
                        None => return Volume::Unreadable("RAR5 data size overflows"),
                    };
                    continue;
                }
                if flags & R5_SPLIT_BEFORE != 0 {
                    return Volume::NotFirst;
                }
                if extra > 0
                    && has_encryption_record(&h[header_end - (extra as usize).min(header_end)..])
                {
                    return Volume::Encrypted;
                }
                // Compression information: bits 7-9 are the method, 0 = store.
                if (compression >> 7) & 0x7 != 0 {
                    return Volume::Compressed;
                }
                return Volume::Stored {
                    name,
                    unpacked,
                    data_offset: header_end as u64,
                    packed: data,
                };
            }
            R5_END => return Volume::Unreadable("RAR5 archive ended before a file"),
            // The main header (and service headers such as comments) carry
            // nothing the probe needs; step over them and their data.
            R5_MAIN => {}
            _ => {}
        }
        at = match header_end.checked_add(data as usize) {
            Some(n) => n,
            None => return Volume::Unreadable("RAR5 data size overflows"),
        };
    }
    Volume::Unreadable("RAR5: no file header in the first 64 headers")
}

/// Walk a RAR5 extra area for a file-encryption record.
fn has_encryption_record(extra: &[u8]) -> bool {
    let mut p = 0;
    while p < extra.len() {
        let start = p;
        let (Some(size), Some(kind)) = (vint(extra, &mut p), vint(extra, &mut p)) else {
            return false;
        };
        if kind == R5_EXTRA_ENCRYPTION {
            return true;
        }
        // size counts from after the size field itself.
        let mut after_size = start;
        let _ = vint(extra, &mut after_size);
        match after_size.checked_add(size as usize) {
            Some(n) if n > p => p = n,
            _ => return false,
        }
    }
    false
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A RAR4 volume header + one stored file header, as WinRAR writes them.
    pub(crate) fn rar4_volume(name: &str, data: &[u8], flags: u16, method: u8) -> Vec<u8> {
        let mut v = RAR4_SIG.to_vec();
        // main header: crc, type, flags (volume|first volume), size 13
        v.extend_from_slice(&[0, 0, R4_MAIN, 0x01, 0x01, 13, 0]);
        v.extend_from_slice(&[0; 6]);
        let size = (7 + 25 + name.len()) as u16;
        v.extend_from_slice(&[0, 0, R4_FILE]);
        v.extend_from_slice(&(flags | 0x8000).to_le_bytes());
        v.extend_from_slice(&size.to_le_bytes());
        v.extend_from_slice(&(data.len() as u32).to_le_bytes()); // PACK_SIZE
        v.extend_from_slice(&(data.len() as u32 * 3).to_le_bytes()); // UNP_SIZE
        v.push(2); // HOST_OS
        v.extend_from_slice(&[0; 4]); // FILE_CRC
        v.extend_from_slice(&[0; 4]); // FTIME
        v.push(29); // UNP_VER
        v.push(method);
        v.extend_from_slice(&(name.len() as u16).to_le_bytes());
        v.extend_from_slice(&[0x20, 0, 0, 0]); // ATTR
        v.extend_from_slice(name.as_bytes());
        v.extend_from_slice(data);
        v
    }

    fn put_vint(v: &mut Vec<u8>, mut n: u64) {
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                v.push(byte);
                return;
            }
            v.push(byte | 0x80);
        }
    }

    fn rar5_header(kind: u64, flags: u64, body: &[u8], extra: &[u8], data: u64) -> Vec<u8> {
        let mut h = Vec::new();
        put_vint(&mut h, kind);
        put_vint(
            &mut h,
            flags
                | if extra.is_empty() { 0 } else { R5_HAS_EXTRA }
                | if data > 0 { R5_HAS_DATA } else { 0 },
        );
        if !extra.is_empty() {
            put_vint(&mut h, extra.len() as u64);
        }
        if data > 0 {
            put_vint(&mut h, data);
        }
        h.extend_from_slice(body);
        h.extend_from_slice(extra);
        let mut out = vec![0, 0, 0, 0];
        put_vint(&mut out, h.len() as u64);
        out.extend_from_slice(&h);
        out
    }

    /// A RAR5 volume: main header then one file header and its data.
    pub(crate) fn rar5_volume(
        name: &str,
        data: &[u8],
        header_flags: u64,
        method: u64,
        encrypted: bool,
    ) -> Vec<u8> {
        let mut v = RAR5_SIG.to_vec();
        v.extend(rar5_header(R5_MAIN, 0, &[0x01], &[], 0)); // archive flags: volume
        let mut body = Vec::new();
        put_vint(&mut body, 0); // file flags
        put_vint(&mut body, data.len() as u64 * 3); // unpacked
        put_vint(&mut body, 0x20); // attributes
        put_vint(&mut body, method << 7); // compression info
        put_vint(&mut body, 0); // host OS
        put_vint(&mut body, name.len() as u64);
        body.extend_from_slice(name.as_bytes());
        let extra = if encrypted {
            vec![2, R5_EXTRA_ENCRYPTION as u8, 0]
        } else {
            Vec::new()
        };
        v.extend(rar5_header(
            R5_FILE,
            header_flags,
            &body,
            &extra,
            data.len() as u64,
        ));
        v.extend_from_slice(data);
        v
    }

    #[test]
    fn rar4_stored_first_volume() {
        let v = rar4_volume("movie.mkv", b"\x1a\x45\xdf\xa3payload", 0, R4_METHOD_STORE);
        match inspect(&v) {
            Volume::Stored {
                name,
                data_offset,
                packed,
                unpacked,
            } => {
                assert_eq!(name, "movie.mkv");
                assert_eq!(
                    &v[data_offset as usize..data_offset as usize + 4],
                    b"\x1a\x45\xdf\xa3"
                );
                assert_eq!(packed, 11);
                assert_eq!(unpacked, 33);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rar4_refusals() {
        assert_eq!(
            inspect(&rar4_volume(
                "m.mkv",
                b"x",
                R4_FILE_SPLIT_BEFORE as u16,
                R4_METHOD_STORE
            )),
            Volume::NotFirst
        );
        assert_eq!(
            inspect(&rar4_volume(
                "m.mkv",
                b"x",
                R4_FILE_PASSWORD as u16,
                R4_METHOD_STORE
            )),
            Volume::Encrypted
        );
        assert_eq!(
            inspect(&rar4_volume("m.mkv", b"x", 0, 0x33)),
            Volume::Compressed
        );
    }

    #[test]
    fn rar5_stored_first_volume() {
        let v = rar5_volume("movie.mkv", b"\x1a\x45\xdf\xa3payload", 0, 0, false);
        match inspect(&v) {
            Volume::Stored {
                name,
                data_offset,
                packed,
                unpacked,
            } => {
                assert_eq!(name, "movie.mkv");
                assert_eq!(
                    &v[data_offset as usize..data_offset as usize + 4],
                    b"\x1a\x45\xdf\xa3"
                );
                assert_eq!(packed, 11);
                assert_eq!(unpacked, 33);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rar5_refusals() {
        assert_eq!(
            inspect(&rar5_volume("m.mkv", b"x", R5_SPLIT_BEFORE, 0, false)),
            Volume::NotFirst
        );
        assert_eq!(
            inspect(&rar5_volume("m.mkv", b"x", 0, 3, false)),
            Volume::Compressed
        );
        assert_eq!(
            inspect(&rar5_volume("m.mkv", b"x", 0, 0, true)),
            Volume::Encrypted
        );
        let mut enc = RAR5_SIG.to_vec();
        enc.extend(rar5_header(R5_ENCRYPTION, 0, &[0, 0], &[], 0));
        assert_eq!(inspect(&enc), Volume::Encrypted);
    }

    #[test]
    fn never_panics_on_prefixes_or_noise() {
        for v in [
            rar4_volume("movie.mkv", b"0123456789", 0, R4_METHOD_STORE),
            rar5_volume("movie.mkv", b"0123456789", 0, 0, false),
        ] {
            for n in 0..v.len() {
                let _ = inspect(&v[..n]);
            }
            let mut noisy = v.clone();
            for i in 8..noisy.len() {
                noisy[i] = noisy[i].wrapping_mul(31).wrapping_add(7);
                let _ = inspect(&noisy);
            }
        }
        assert_eq!(inspect(b"\x1a\x45\xdf\xa3"), Volume::NotRar);
    }

    /// What the fuzz target holds: a stored member's data starts inside the
    /// bytes inspected, for every prefix of a real volume.
    #[test]
    fn stored_data_starts_inside_the_bytes() {
        for v in [
            rar4_volume("movie.mkv", b"0123456789", 0, R4_METHOD_STORE),
            rar5_volume("movie.mkv", b"0123456789", 0, 0, false),
        ] {
            for n in 0..=v.len() {
                if let Volume::Stored { data_offset, .. } = inspect(&v[..n]) {
                    assert!(
                        data_offset as usize <= n,
                        "prefix {n}: data at {data_offset}"
                    );
                }
            }
        }
    }
}
