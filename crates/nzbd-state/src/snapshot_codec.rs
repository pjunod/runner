//! Lossless on-disk encoding only; queue document and replay semantics stay intact.
use crate::StateError;
use std::borrow::Cow;
use std::io::{Read, Write};

const MAGIC: &[u8; 8] = b"NZBDQZ01";
const HEADER_LEN: usize = 20;
pub(super) const MAGIC_V2: &[u8; 8] = b"NZBDQZ02";
pub(super) const MIN_BYTES: usize = 1024 * 1024;
// Bound decompression without imposing a new limit on legacy JSON queues.
// Larger documents keep their existing plain JSON representation.
pub(super) const MAX_BYTES: usize = 1024 * 1024 * 1024;

fn corrupt(message: impl std::fmt::Display) -> StateError {
    StateError::Corrupt(format!("queue.json compressed snapshot: {message}"))
}

pub(super) fn encode(bytes: Vec<u8>) -> Result<Vec<u8>, StateError> {
    if !(MIN_BYTES..=MAX_BYTES).contains(&bytes.len()) {
        return Ok(bytes);
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let encoded = encode_frame_into(&bytes, encoded)?;
    // Do not increase filesystem traffic for incompressible input.
    Ok(if encoded.len() < bytes.len() {
        encoded
    } else {
        bytes
    })
}

pub(super) fn encode_frame(bytes: &[u8]) -> Result<Vec<u8>, StateError> {
    encode_frame_into(bytes, Vec::new())
}

fn encode_frame_into(bytes: &[u8], output: Vec<u8>) -> Result<Vec<u8>, StateError> {
    let mut encoder = zstd::stream::write::Encoder::new(output, 1).map_err(corrupt)?;
    encoder.include_checksum(true).map_err(corrupt)?;
    encoder
        .set_pledged_src_size(Some(bytes.len() as u64))
        .map_err(corrupt)?;
    encoder.write_all(bytes).map_err(corrupt)?;
    encoder.finish().map_err(corrupt)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Cow<'_, [u8]>, StateError> {
    if !bytes.starts_with(b"NZBDQZ") {
        return Ok(Cow::Borrowed(bytes));
    }
    let (header_len, frames) = if bytes.starts_with(MAGIC) {
        (HEADER_LEN, 1)
    } else if bytes.starts_with(MAGIC_V2) {
        if bytes.len() < 24 {
            return Err(corrupt("truncated header"));
        }
        let frames = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        if !(2..=4098).contains(&frames) {
            return Err(corrupt("invalid frame count"));
        }
        (24, frames)
    } else {
        return Err(corrupt("unsupported or truncated encoding version"));
    };
    if bytes.len() < header_len {
        return Err(corrupt("truncated header"));
    }
    let size = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if size > MAX_BYTES as u64 {
        return Err(corrupt("declared output exceeds the 1 GiB encoding limit"));
    }
    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let mut remaining = &bytes[header_len..];
    let mut decoded = Vec::new();
    for _ in 0..frames {
        let mut decoder = zstd::stream::read::Decoder::with_buffer(remaining)
            .map_err(corrupt)?
            .single_frame();
        decoder.window_log_max(30).map_err(corrupt)?;
        (&mut decoder)
            .take(size - decoded.len() as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(corrupt)?;
        if decoded.len() as u64 > size {
            return Err(corrupt("decoded length exceeds header"));
        }
        let next = decoder.finish();
        if next.len() == remaining.len() {
            return Err(corrupt("missing compressed frame"));
        }
        remaining = next;
    }
    if decoded.len() as u64 != size {
        return Err(corrupt("decoded length does not match header"));
    }
    if !remaining.is_empty() {
        return Err(corrupt("trailing data after compressed frame"));
    }
    if crc32fast::hash(&decoded) != expected_crc {
        return Err(corrupt("decoded checksum does not match header"));
    }
    Ok(Cow::Owned(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> Vec<u8> {
        br#"{"message_id":"same-message-id","state":"pending"},"#.repeat(MIN_BYTES / 48 + 1)
    }

    #[test]
    fn compressed_encoding_is_an_exact_bounded_roundtrip() {
        let raw = raw();
        let encoded = encode(raw.clone()).unwrap();
        assert!(encoded.starts_with(MAGIC));
        assert!(encoded.len() < raw.len() / 4);
        assert_eq!(decode(&encoded).unwrap().as_ref(), raw);
        assert!(matches!(
            decode(b"{\"legacy\":true}").unwrap(),
            Cow::Borrowed(_)
        ));
        assert_eq!(encode(b"small".to_vec()).unwrap(), b"small");
    }

    #[test]
    fn malformed_frames_fail_closed() {
        let encoded = encode(raw()).unwrap();
        for end in 6..encoded.len() {
            assert!(decode(&encoded[..end]).is_err(), "truncation at {end}");
        }
        let mut bad_version = encoded.clone();
        bad_version[7] = b'3';
        assert!(decode(&bad_version).is_err());
        let mut oversized = encoded.clone();
        oversized[8..16].copy_from_slice(&(MAX_BYTES as u64 + 1).to_le_bytes());
        assert!(decode(&oversized).is_err());
        for size in [0, 1, raw().len() as u64 - 1, raw().len() as u64 + 1] {
            let mut wrong_size = encoded.clone();
            wrong_size[8..16].copy_from_slice(&size.to_le_bytes());
            assert!(decode(&wrong_size).is_err());
        }
        let mut wrong_crc = encoded.clone();
        wrong_crc[16] ^= 1;
        assert!(decode(&wrong_crc).is_err());
        let mut damaged = encoded.clone();
        *damaged.last_mut().unwrap() ^= 1;
        assert!(decode(&damaged).is_err());
        for suffix in [b"garbage".as_slice(), &encoded[HEADER_LEN..]] {
            let mut extra = encoded.clone();
            extra.extend_from_slice(suffix);
            assert!(decode(&extra).is_err());
        }
    }
}
