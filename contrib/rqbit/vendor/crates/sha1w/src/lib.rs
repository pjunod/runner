// SHA-1 is mandated by BitTorrent v1. The historical default feature now uses
// maintained RustCrypto; the daemon's sha1-ring/AWS-LC backend is unchanged.

pub trait ISha1 {
    fn new() -> Self;
    fn update(&mut self, buf: &[u8]);
    fn finish(self) -> [u8; 20];
}

assert_cfg::exactly_one! {
    feature = "sha1-crypto-hash",
    feature = "sha1-ring",
}

#[cfg(feature = "sha1-crypto-hash")]
mod portable_impl {
    use super::ISha1;
    use sha1::{Digest, Sha1};
    pub struct Sha1Portable {
        inner: Sha1,
    }
    impl ISha1 for Sha1Portable {
        fn new() -> Self {
            Self { inner: Sha1::new() }
        }
        fn update(&mut self, buf: &[u8]) {
            self.inner.update(buf);
        }
        fn finish(self) -> [u8; 20] {
            self.inner.finalize().into()
        }
    }
}

#[cfg(feature = "sha1-ring")]
mod ring_impl {
    use super::ISha1;

    use aws_lc_rs::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY as SHA1};

    pub struct Sha1Ring {
        ctx: Context,
    }

    impl ISha1 for Sha1Ring {
        fn new() -> Self {
            Self {
                ctx: Context::new(&SHA1),
            }
        }

        fn update(&mut self, buf: &[u8]) {
            self.ctx.update(buf);
        }

        fn finish(self) -> [u8; 20] {
            let result = self.ctx.finish();
            debug_assert_eq!(result.as_ref().len(), 20);
            let mut result_arr = [0u8; 20];
            result_arr.copy_from_slice(result.as_ref());
            result_arr
        }
    }
}

#[cfg(feature = "sha1-crypto-hash")]
pub type Sha1 = portable_impl::Sha1Portable;

#[cfg(feature = "sha1-ring")]
pub type Sha1 = ring_impl::Sha1Ring;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sha1_protocol_vectors_match_for_whole_and_segmented_input() {
        for (input, expected) in [
            (b"".as_slice(), "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            (
                b"abc".as_slice(),
                "a9993e364706816aba3e25717850c26c9cd0d89d",
            ),
            (
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".as_slice(),
                "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            ),
        ] {
            for chunk in [1, 7, 55, 56, 63, 64, 65] {
                let mut hash = Sha1::new();
                for bytes in input.chunks(chunk) {
                    hash.update(bytes);
                }
                let result: String = hash.finish().iter().map(|b| format!("{b:02x}")).collect();
                assert_eq!(result, expected);
            }
        }
    }
}
