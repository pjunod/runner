#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    // Exercise the actual public storage boundary, including native untrusted
    // lengths. Successful decoding must preserve every consumed legacy byte;
    // the established API deliberately permits trailing bytes.
    if let Ok(keys) = cryptr::EncKeys::try_from(input) {
        let bytes = bincode::encode_to_vec(&keys, bincode::config::legacy()).unwrap();
        assert!(input.starts_with(&bytes));
        assert_eq!(cryptr::EncKeys::try_from(bytes.as_slice()).unwrap(), keys);
    }
});
