#![no_main]
//! The release probe reads RAR headers from bytes a stranger posted to
//! usenet. It must never panic, and a stored member's data range must lie
//! inside the bytes it was handed.

use libfuzzer_sys::fuzz_target;
use nzbd_probe::rar::{inspect, Volume};

fuzz_target!(|data: &[u8]| {
    if let Volume::Stored { data_offset, .. } = inspect(data) {
        assert!(data_offset as usize <= data.len());
    }
});
