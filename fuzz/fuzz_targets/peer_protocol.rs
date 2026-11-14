#![no_main]

use buffers::ByteBuf;
use libfuzzer_sys::fuzz_target;
use peer::{Handshake, Message};

fuzz_target!(|input: &[u8]| {
    if let Ok((handshake, consumed)) = Handshake::deserialize(input) {
        let mut encoded = Vec::new();
        handshake.serialize(&mut encoded);
        assert_eq!(encoded, input[..consumed]);
    }
    let _ = Message::<ByteBuf<'_>>::deserialize(input);
});
