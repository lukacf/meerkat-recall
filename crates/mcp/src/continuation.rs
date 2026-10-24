//! Sealed continuation tokens for paged reads.
//!
//! A continuation carries server state between pages: where the previous page
//! stopped, and the window it was read against. That position can belong to a
//! row the caller is not allowed to see (a page cut by a scan ceiling resumes
//! after the last row scanned, visible or not), so the token must conceal it, and
//! it must only resume the read it was issued for.
//!
//! Tokens are sealed with ChaCha20-Poly1305 under a server-side key: confidential
//! and tamper-evident. The caller-specific `binding` (principal, space, request
//! parameters) is authenticated as associated data, so a token opened by another
//! principal, in another space or for other parameters fails exactly like a
//! forged one. Opening a token proves only that this server issued it for this
//! binding; every page is still authorized afresh.

use std::sync::{Arc, OnceLock};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305, NONCE_LEN};
use ring::hkdf::{Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};

const TOKEN_PREFIX: &str = "c1.";
const KEY_INFO: &[u8] = b"meerkat-recall/continuation/v1";
const AAD_DOMAIN: &[u8] = b"meerkat-recall/continuation/v1\0";

/// Server-side key that seals and opens continuation tokens.
pub struct ContinuationKey {
    key: LessSafeKey,
    rng: SystemRandom,
}

impl std::fmt::Debug for ContinuationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuationKey").finish_non_exhaustive()
    }
}

impl ContinuationKey {
    /// Derives the key from existing server secret material (HKDF-SHA256 with a
    /// purpose label, so the token key never equals the secret it came from).
    #[must_use]
    pub fn from_secret(secret: &[u8]) -> Self {
        let prk = Salt::new(HKDF_SHA256, &[]).extract(secret);
        // `expand` only fails for an output longer than 255 hash blocks; a
        // 32-byte AEAD key never is.
        let key = prk.expand(&[KEY_INFO], &CHACHA20_POLY1305).map_or_else(
            |_| unreachable!("a 32-byte HKDF output is always available"),
            UnboundKey::from,
        );
        Self {
            key: LessSafeKey::new(key),
            rng: SystemRandom::new(),
        }
    }

    /// A random key for this process, used when no server secret was supplied
    /// (skip-auth development, embedded hosts, tests). Its tokens stop opening
    /// after a restart, which only ends a paged read early.
    #[must_use]
    pub fn process_local() -> Arc<Self> {
        static KEY: OnceLock<Arc<ContinuationKey>> = OnceLock::new();
        KEY.get_or_init(|| {
            let mut secret = [0_u8; 32];
            SystemRandom::new()
                .fill(&mut secret)
                .unwrap_or_else(|_| unreachable!("the system RNG is available"));
            Arc::new(Self::from_secret(&secret))
        })
        .clone()
    }

    /// Seals `plaintext` for `binding`. A fresh random nonce per token; the
    /// birthday bound of a 96-bit nonce is far beyond any continuation volume.
    #[must_use]
    pub fn seal(&self, binding: &[u8], plaintext: &[u8]) -> Option<String> {
        let mut nonce = [0_u8; NONCE_LEN];
        self.rng.fill(&mut nonce).ok()?;
        let mut sealed = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad(binding)),
                &mut sealed,
            )
            .ok()?;
        let mut token = nonce.to_vec();
        token.extend_from_slice(&sealed);
        Some(format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(token)))
    }

    /// Opens a token sealed for exactly this `binding`; `None` for anything else
    /// (malformed, tampered, another key, another binding).
    #[must_use]
    pub fn open(&self, binding: &[u8], token: &str) -> Option<Vec<u8>> {
        let raw = URL_SAFE_NO_PAD
            .decode(token.strip_prefix(TOKEN_PREFIX)?)
            .ok()?;
        if raw.len() < NONCE_LEN + CHACHA20_POLY1305.tag_len() {
            return None;
        }
        let (nonce, sealed) = raw.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
        let mut sealed = sealed.to_vec();
        let plaintext = self
            .key
            .open_in_place(nonce, Aad::from(aad(binding)), &mut sealed)
            .ok()?;
        Some(plaintext.to_vec())
    }
}

fn aad(binding: &[u8]) -> Vec<u8> {
    let mut aad = AAD_DOMAIN.to_vec();
    aad.extend_from_slice(binding);
    aad
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_sealed_token_opens_only_for_its_binding_and_key() {
        let key = ContinuationKey::from_secret(b"a server secret of at least 32 bytes");
        let token = key.seal(b"binding-a", b"position").expect("seal");

        assert_eq!(
            key.open(b"binding-a", &token).as_deref(),
            Some(&b"position"[..])
        );
        assert!(key.open(b"binding-b", &token).is_none());
        let other = ContinuationKey::from_secret(b"another server secret, also long");
        assert!(other.open(b"binding-a", &token).is_none());
    }

    #[test]
    fn a_tampered_or_malformed_token_does_not_open() {
        let key = ContinuationKey::from_secret(b"a server secret of at least 32 bytes");
        let token = key.seal(b"b", b"position").expect("seal");

        let mut flipped = token.clone().into_bytes();
        let last = flipped.len() - 3;
        flipped[last] = if flipped[last] == b'A' { b'B' } else { b'A' };
        let flipped = String::from_utf8(flipped).expect("ascii");
        for bad in [
            flipped.as_str(),
            &token[..token.len() - 4],
            "c1.",
            "c1.not base64!",
            "position",
            &token[TOKEN_PREFIX.len()..],
        ] {
            assert!(key.open(b"b", bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn tokens_for_the_same_position_differ() {
        let key = ContinuationKey::from_secret(b"a server secret of at least 32 bytes");
        assert_ne!(key.seal(b"b", b"p"), key.seal(b"b", b"p"));
    }
}
