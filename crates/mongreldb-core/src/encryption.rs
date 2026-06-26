//! Native page-level encryption (optional, behind the `encryption` feature).
//!
//! Realizes §7 of the design spec (Phase 10.1): a per-table Key-Encryption Key
//! (KEK) derived from a passphrase via **Argon2id** (slow memory-hard KDF,
//! resists offline brute force) followed by **HKDF-SHA256** expand (domain
//! separation). Each sorted run gets a fresh random Data-Encryption Key (DEK);
//! the DEK is wrapped (AES-256-GCM) by the KEK and stored, alongside a per-run
//! nonce prefix, in the run's Encryption Descriptor. Per-page nonces are
//! deterministic — `nonce_prefix[0..8] (random) || column_id (2) || page_seq
//! (2)` — so no per-page nonce material is persisted. Decrypting a page
//! requires unwrapping its run's DEK with the table KEK.

#[cfg(feature = "encryption")]
use crate::MongrelError;
use crate::Result;

/// Symmetric page cipher.
pub trait Cipher: Send + Sync {
    /// Encrypt a page payload. `nonce` is the deterministic per-page nonce.
    fn encrypt_page(&self, nonce: &[u8; 12], plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt a page payload.
    fn decrypt_page(&self, nonce: &[u8; 12], ciphertext: &[u8]) -> Result<Vec<u8>>;
}

/// No-op cipher for unencrypted tables. Used by default.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlaintextCipher;

impl Cipher for PlaintextCipher {
    fn encrypt_page(&self, _nonce: &[u8; 12], plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt_page(&self, _nonce: &[u8; 12], ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }
}

#[cfg(feature = "encryption")]
mod aes {
    use super::{Cipher, MongrelError, Result};
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

    /// AES-256-GCM page cipher over an (unwrapped) per-run DEK. Per-page nonces
    /// are derived outside and passed in.
    pub struct AesCipher {
        cipher: Aes256Gcm,
    }

    impl AesCipher {
        /// `key` must be exactly 32 bytes (the unwrapped DEK).
        pub fn new(key: &[u8]) -> Result<Self> {
            if key.len() != 32 {
                return Err(MongrelError::InvalidArgument(format!(
                    "aes-256 key must be 32 bytes, got {}",
                    key.len()
                )));
            }
            Ok(Self {
                cipher: Aes256Gcm::new_from_slice(key)
                    .map_err(|e| MongrelError::Encryption(format!("aes key init: {e}")))?,
            })
        }
    }

    impl Cipher for AesCipher {
        fn encrypt_page(&self, nonce: &[u8; 12], plaintext: &[u8]) -> Result<Vec<u8>> {
            let nonce = Nonce::from_slice(nonce);
            self.cipher
                .encrypt(nonce, plaintext)
                .map_err(|e| MongrelError::Encryption(format!("aes encrypt: {e}")))
        }

        fn decrypt_page(&self, nonce: &[u8; 12], ciphertext: &[u8]) -> Result<Vec<u8>> {
            let nonce = Nonce::from_slice(nonce);
            self.cipher
                .decrypt(nonce, ciphertext)
                .map_err(|e| MongrelError::Decryption(format!("aes decrypt: {e}")))
        }
    }
}

#[cfg(feature = "encryption")]
pub use aes::AesCipher;

#[cfg(feature = "encryption")]
mod key {
    use super::{Cipher, MongrelError, Result};
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};
    use serde::{Deserialize, Serialize};
    use zeroize::Zeroizing;

    /// Argon2id salt length (bytes).
    pub const SALT_LEN: usize = 16;
    /// DEK length (AES-256 = 32 bytes).
    pub const DEK_LEN: usize = 32;
    /// Algorithm tag stored in the Encryption Descriptor: AES-256-GCM.
    pub const ALGO_AES_GCM: u8 = 1;
    /// HKDF-SHA256 info label for KEK domain separation.
    const KEK_INFO: &[u8] = b"mongreldb/kek/v1";
    /// HKDF-SHA256 info label for raw-key KEK domain separation.
    const KEK_RAW_INFO: &[u8] = b"mongreldb/kek-raw/v1";
    /// Argon2id memory cost (KiB) — OWASP-recommended minimum (≈19 MiB).
    const ARGON2_M_COST: u32 = 19_456;
    /// Argon2id time cost (iterations).
    const ARGON2_T_COST: u32 = 2;
    /// Argon2id parallelism.
    const ARGON2_P_COST: u32 = 1;

    /// Table-level Key-Encryption Key. Derived from a passphrase + salt via
    /// Argon2id (the extract step, memory-hard) followed by HKDF-SHA256 expand
    /// (domain separation). Never persisted; reconstructable only from the
    /// passphrase plus the stored salt.
    pub struct Kek(Zeroizing<[u8; DEK_LEN]>);

    impl Kek {
        /// Derive a 256-bit KEK from `passphrase` and `salt` via Argon2id +
        /// HKDF-SHA256.
        pub fn derive(passphrase: &str, salt: &[u8; SALT_LEN]) -> Result<Self> {
            let params =
                argon2::Params::new(ARGON2_M_COST, ARGON2_T_COST, ARGON2_P_COST, Some(DEK_LEN))
                    .map_err(|e| MongrelError::Encryption(format!("argon2 params: {e}")))?;
            let argon =
                argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
            // Argon2id output is the extracted pseudo-random key (PRK).
            let mut prk = Zeroizing::new([0u8; DEK_LEN]);
            argon
                .hash_password_into(passphrase.as_bytes(), salt, prk.as_mut())
                .map_err(|e| MongrelError::Encryption(format!("argon2 derive: {e}")))?;
            // HKDF-Expand gives a domain-separated KEK from the PRK.
            let hk = hkdf::Hkdf::<sha2::Sha256>::from_prk(prk.as_ref())
                .map_err(|e| MongrelError::Encryption(format!("hkdf from_prk: {e}")))?;
            let mut kek = Zeroizing::new([0u8; DEK_LEN]);
            hk.expand(KEK_INFO, kek.as_mut())
                .map_err(|e| MongrelError::Encryption(format!("hkdf expand: {e}")))?;
            Ok(Kek(kek))
        }

        /// Derive a 256-bit KEK from a raw key (e.g. a key file's contents)
        /// via HKDF-SHA256 only — no Argon2id. The raw key must be >= 32
        /// bytes and already high-entropy (machine-generated). ~0.1ms vs
        /// ~50ms for the passphrase path.
        pub fn from_raw_key(raw: &[u8], salt: &[u8; SALT_LEN]) -> Result<Self> {
            if raw.len() < DEK_LEN {
                return Err(MongrelError::InvalidArgument(format!(
                    "raw key must be >= {DEK_LEN} bytes, got {}",
                    raw.len()
                )));
            }
            // HKDF-Extract (uses salt for domain separation), then HKDF-Expand.
            let hk = hkdf::Hkdf::<sha2::Sha256>::new(Some(salt), raw);
            let mut kek = Zeroizing::new([0u8; DEK_LEN]);
            hk.expand(KEK_RAW_INFO, kek.as_mut())
                .map_err(|e| MongrelError::Encryption(format!("hkdf expand: {e}")))?;
            Ok(Kek(kek))
        }

        /// Derive a WAL DEK from this KEK for frame-level AEAD.
        pub fn derive_wal_key(&self) -> Zeroizing<[u8; DEK_LEN]> {
            self.derive_subkey(b"mongreldb/wal/v1")
        }

        /// Derive a result-cache DEK from this KEK for cache file AEAD.
        pub fn derive_cache_key(&self) -> Zeroizing<[u8; DEK_LEN]> {
            self.derive_subkey(b"mongreldb/rcache/v1")
        }

        /// Wrap a 32-byte DEK with the KEK using AES-256-GCM. `wrap_nonce` must
        /// be unique per use under this KEK (a run's random `nonce_prefix`
        /// satisfies this).
        pub fn wrap_dek(&self, dek: &[u8; DEK_LEN], wrap_nonce: &[u8; 12]) -> Result<Vec<u8>> {
            let cipher = Aes256Gcm::new_from_slice(&self.0[..])
                .map_err(|e| MongrelError::Encryption(format!("kek aes init: {e}")))?;
            cipher
                .encrypt(Nonce::from_slice(wrap_nonce), dek.as_slice())
                .map_err(|e| MongrelError::Encryption(format!("dek wrap: {e}")))
        }

        /// Unwrap a DEK previously produced by [`Self::wrap_dek`].
        pub fn unwrap_dek(
            &self,
            wrapped: &[u8],
            wrap_nonce: &[u8; 12],
        ) -> Result<Zeroizing<[u8; DEK_LEN]>> {
            let cipher = Aes256Gcm::new_from_slice(&self.0[..])
                .map_err(|e| MongrelError::Encryption(format!("kek aes init: {e}")))?;
            let pt = cipher
                .decrypt(Nonce::from_slice(wrap_nonce), wrapped)
                .map_err(|e| MongrelError::Decryption(format!("dek unwrap: {e}")))?;
            if pt.len() != DEK_LEN {
                return Err(MongrelError::Decryption(format!(
                    "unwrapped dek is {} bytes, expected {DEK_LEN}",
                    pt.len()
                )));
            }
            let mut dek = Zeroizing::new([0u8; DEK_LEN]);
            dek.copy_from_slice(&pt);
            Ok(dek)
        }
    }

    impl std::fmt::Debug for Kek {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Kek(**redacted**)")
        }
    }

    /// Scheme tag for an indexable-encrypted column (§7).
    pub const SCHEME_HMAC_EQ: u8 = 1;
    pub const SCHEME_OPE_RANGE: u8 = 2;

    impl Kek {
        /// Derive a 256-bit sub-key from this KEK via HKDF-Expand, domain-
        /// separated by `info`. Used for per-column indexable-encryption keys:
        /// deriving deterministically from the (stable) KEK makes a column's
        /// tokens identical across runs, so cross-run indexes (bitmap / range)
        /// unify them.
        pub fn derive_subkey(&self, info: &[u8]) -> Zeroizing<[u8; DEK_LEN]> {
            let hk = hkdf::Hkdf::<sha2::Sha256>::from_prk(&self.0[..])
                .expect("KEK is 32 bytes >= HashLen");
            let mut k = Zeroizing::new([0u8; DEK_LEN]);
            hk.expand(info, k.as_mut())
                .expect("32-byte output <= 255*HashLen");
            k
        }

        /// Derive the per-column indexable-encryption key.
        pub fn derive_column_key(&self, column_id: u16) -> Zeroizing<[u8; DEK_LEN]> {
            let mut info = b"mongreldb/colkey/".to_vec();
            info.extend_from_slice(&column_id.to_be_bytes());
            self.derive_subkey(&info)
        }

        /// Wrap a column key (for the §7 descriptor). Reuses the run nonce prefix.
        pub fn wrap_column_key(
            &self,
            col_key: &[u8; DEK_LEN],
            wrap_nonce: &[u8; 12],
        ) -> Result<Vec<u8>> {
            self.wrap_dek(col_key, wrap_nonce)
        }
    }

    /// Deterministic equality token: HMAC-SHA256 over the value's bytes. Equal
    /// plaintexts collide; unequal plaintexts (cryptographically) do not. Used
    /// for equality indexes (bitmap / PK) over ENCRYPTED_INDEXABLE columns.
    pub fn hmac_token(col_key: &[u8; DEK_LEN], msg: &[u8]) -> [u8; 32] {
        use hmac::Mac;
        let mut mac = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(col_key)
            .expect("HMAC accepts any key size");
        mac.update(msg);
        mac.finalize().into_bytes().into()
    }

    /// Order-preserving token for an `i64`: a key-derived affine map on the
    /// sign-flipped u64 representation, computed in u128 so it never wraps. The
    /// result is a 16-byte big-endian token whose byte order equals the value's
    /// numeric order, so a range index over the token serves range queries
    /// without decrypting. OPE inherently leaks order; the slope/intercept are
    /// secret (key-derived), so absolute values stay hidden.
    pub fn ope_token_i64(col_key: &[u8; DEK_LEN], x: i64) -> [u8; 16] {
        let m = (x as u64) ^ (1u64 << 63); // order-preserving i64 -> u64
        affine_ope(col_key, m)
    }

    /// Order-preserving token for an `f64`, via the IEEE-754 total-order → u64
    /// bijection, then the same affine OPE as [`ope_token_i64`].
    pub fn ope_token_f64(col_key: &[u8; DEK_LEN], x: f64) -> [u8; 16] {
        let bits = x.to_bits();
        let m = if bits & (1u64 << 63) != 0 {
            !bits
        } else {
            bits ^ (1u64 << 63)
        };
        affine_ope(col_key, m)
    }

    /// key-derived affine OPE on a u64 domain: `token = a*m + b` in u128, where
    /// `a` is a positive odd 32-bit value and `b` a 64-bit value, both derived
    /// from the column key. `a < 2^32` and `m < 2^64` ⇒ `a*m + b < 2^96`, so it
    /// never wraps and is strictly increasing in `m` (injective).
    fn affine_ope(col_key: &[u8; DEK_LEN], m: u64) -> [u8; 16] {
        let a = (u64::from_be_bytes(col_key[0..8].try_into().unwrap()) & 0xFFFF_FFFF) | 1;
        let b = u64::from_be_bytes(col_key[8..16].try_into().unwrap());
        let token = (m as u128) * (a as u128) + (b as u128);
        token.to_be_bytes()
    }

    /// Build a distinct AEAD nonce for a KEK wrap (DEK or a column key) from the
    /// run's `nonce_prefix`. The high 8 bytes stay random-per-run (cross-run
    /// uniqueness under the KEK); byte `[8]` distinguishes the wrap kind (0 =
    /// DEK, 1 = column key) and bytes `[9..11]` carry the column id, so the DEK
    /// and every column key get a UNIQUE nonce within one run — mandatory for
    /// AES-GCM (nonce reuse under a key is catastrophic).
    pub(super) fn wrap_nonce(nonce_prefix: [u8; 12], kind: u8, column_id: u16) -> [u8; 12] {
        let mut n = nonce_prefix;
        n[8] = kind;
        n[9..11].copy_from_slice(&column_id.to_le_bytes());
        n[11] = 0;
        n
    }

    /// Wrap-nonce kind tag for the DEK (also used to unwrap on read).
    pub(super) const WRAP_KIND_DEK: u8 = 0;
    /// Wrap-nonce kind tag for a column key.
    pub(super) const WRAP_KIND_COLUMN: u8 = 1;

    /// Per-column indexable-encryption descriptor (Phase 10.2 — HMAC-eq /
    /// OPE-range). Populated for every ENCRYPTED_INDEXABLE column so the
    /// descriptor is self-describing per §7.
    #[derive(Clone, Serialize, Deserialize)]
    pub struct ColumnKeyDescriptor {
        pub column_id: u16,
        /// 1 = HMAC-eq, 2 = OPE-range.
        pub scheme: u8,
        pub wrapped_column_key: Vec<u8>,
    }

    /// Encryption Descriptor, serialized into each encrypted run at
    /// `header.encryption_descriptor_offset` (4-byte little-endian length prefix
    /// + bincode body).
    #[derive(Clone, Serialize, Deserialize)]
    pub struct EncryptionDescriptor {
        /// 1 = AES-256-GCM.
        pub algo: u8,
        /// 12-byte per-run nonce prefix. Bytes `[0..8]` are random per run;
        /// bytes `[8..12]` are zero and overlaid with `column_id` + `page_seq`
        /// at the page level (see [`build_page_nonce`]).
        pub nonce_prefix: [u8; 12],
        /// DEK wrapped by the table KEK (AES-256-GCM; 32 + 16-byte tag = 48).
        pub wrapped_dek: Vec<u8>,
        /// Per-column indexable-encryption descriptors (Phase 10.2).
        pub column_descriptors: Vec<ColumnKeyDescriptor>,
    }

    /// Generate a fresh random 32-byte DEK from the OS CSPRNG.
    pub fn generate_dek() -> Zeroizing<[u8; DEK_LEN]> {
        let mut k = Zeroizing::new([0u8; DEK_LEN]);
        fill_random(k.as_mut());
        k
    }

    /// Generate a fresh random 16-byte Argon2id salt from the OS CSPRNG.
    pub fn random_salt() -> [u8; SALT_LEN] {
        let mut s = [0u8; SALT_LEN];
        fill_random(&mut s);
        s
    }

    /// Generate a per-run nonce prefix: 8 random bytes + 4 zero bytes (the low
    /// 4 bytes are overlaid per page by [`build_page_nonce`]).
    pub fn random_nonce_prefix() -> [u8; 12] {
        let mut n = [0u8; 12];
        fill_random(&mut n[..8]);
        n
    }

    /// Construct the deterministic 12-byte nonce for a page:
    /// `nonce_prefix[0..8] (random, per run) || column_id (2) || page_seq (2)`.
    /// Within a run the `(column_id, page_seq)` pair is unique per page and the
    /// DEK is per-run, so nonces never repeat under a key.
    pub fn build_page_nonce(nonce_prefix: [u8; 12], column_id: u16, page_seq: u32) -> [u8; 12] {
        let mut n = nonce_prefix;
        n[8..10].copy_from_slice(&column_id.to_le_bytes());
        // page_seq occupies the low 2 bytes — a column/run cannot exceed
        // 65 535 pages (enforced at encode), so this never truncates.
        n[10..12].copy_from_slice(&(page_seq as u16).to_le_bytes());
        n
    }

    /// Assemble per-run encryption material from a KEK: generate a DEK, build
    /// the page cipher, wrap the DEK into a descriptor, and derive+wrap a per-
    /// column key for each ENCRYPTED_INDEXABLE column (`(column_id, scheme)`).
    /// Each KEK wrap uses a distinct nonce (DEK vs column keys) so AES-GCM never
    /// sees nonce reuse under the KEK.
    pub fn setup_run_encryption(
        kek: &Kek,
        indexable_columns: &[(u16, u8)],
    ) -> Result<crate::encryption::RunEncryption> {
        let dek = generate_dek();
        let nonce_prefix = random_nonce_prefix();
        let cipher: Box<dyn Cipher> = Box::new(crate::encryption::AesCipher::new(&dek[..])?);
        let dek_nonce = wrap_nonce(nonce_prefix, WRAP_KIND_DEK, 0);
        let wrapped = kek.wrap_dek(&dek, &dek_nonce)?;
        let mut column_descriptors = Vec::with_capacity(indexable_columns.len());
        for &(column_id, scheme) in indexable_columns {
            let col_key = kek.derive_column_key(column_id);
            let col_nonce = wrap_nonce(nonce_prefix, WRAP_KIND_COLUMN, column_id);
            let wrapped_col = kek.wrap_column_key(&col_key, &col_nonce)?;
            column_descriptors.push(ColumnKeyDescriptor {
                column_id,
                scheme,
                wrapped_column_key: wrapped_col,
            });
        }
        let desc = EncryptionDescriptor {
            algo: ALGO_AES_GCM,
            nonce_prefix,
            wrapped_dek: wrapped,
            column_descriptors,
        };
        let descriptor_bytes = bincode::serialize(&desc)?;
        Ok(crate::encryption::RunEncryption {
            cipher,
            nonce_prefix,
            descriptor_bytes,
        })
    }

    /// Read a run's Encryption Descriptor (serialized body, not the length
    /// prefix) and unwrap its DEK, returning the page cipher + nonce prefix.
    pub fn build_run_cipher(
        kek: &Kek,
        descriptor_bytes: &[u8],
    ) -> Result<crate::encryption::RunEncryption> {
        let desc: EncryptionDescriptor = bincode::deserialize(descriptor_bytes)
            .map_err(|e| MongrelError::Decryption(format!("bad encryption descriptor: {e}")))?;
        if desc.algo != ALGO_AES_GCM {
            return Err(MongrelError::Decryption(format!(
                "unsupported encryption algo {}",
                desc.algo
            )));
        }
        let dek_nonce = wrap_nonce(desc.nonce_prefix, WRAP_KIND_DEK, 0);
        let dek = kek.unwrap_dek(&desc.wrapped_dek, &dek_nonce)?;
        let cipher: Box<dyn Cipher> = Box::new(crate::encryption::AesCipher::new(&dek[..])?);
        Ok(crate::encryption::RunEncryption {
            cipher,
            nonce_prefix: desc.nonce_prefix,
            descriptor_bytes: Vec::new(),
        })
    }

    fn fill_random(buf: &mut [u8]) {
        // The OS CSPRNG must not fail in practice; a failure is fatal for
        // security-critical key material.
        getrandom::getrandom(buf).expect("getrandom: OS CSPRNG unavailable");
    }
}

/// Public wrapper for `fill_random` — fills a buffer with OS CSPRNG bytes.
#[cfg(feature = "encryption")]
pub fn fill_random_pub(buf: &mut [u8]) {
    getrandom::getrandom(buf).expect("getrandom: OS CSPRNG unavailable");
}

#[cfg(feature = "encryption")]
pub use key::{
    build_page_nonce, build_run_cipher, generate_dek, hmac_token, ope_token_f64, ope_token_i64,
    random_nonce_prefix, random_salt, setup_run_encryption, ColumnKeyDescriptor,
    EncryptionDescriptor, Kek, ALGO_AES_GCM, DEK_LEN, SALT_LEN, SCHEME_HMAC_EQ, SCHEME_OPE_RANGE,
};

/// Per-run encryption material assembled at write/read time: the page cipher
/// (over the unwrapped DEK), the nonce prefix, and (write path only) the
/// serialized descriptor to embed in the run. Carries only trait-object +
/// primitive fields so it is constructible without the `encryption` feature's
/// concrete crypto types.
pub struct RunEncryption {
    pub cipher: Box<dyn Cipher>,
    pub nonce_prefix: [u8; 12],
    pub descriptor_bytes: Vec<u8>,
}

/// Placeholder KEK when the `encryption` feature is disabled. It has no public
/// constructor, so it can never exist — encrypted tables are therefore
/// impossible without the feature, and all plumbing types
/// (`Option<Arc<Kek>>` etc.) remain valid without a cfg gate.
#[cfg(not(feature = "encryption"))]
pub struct Kek {
    _private: (),
}

/// Unreachable stub — a [`Kek`] cannot be constructed without the `encryption`
/// feature, so the writer never reaches this path when encryption is disabled.
#[cfg(not(feature = "encryption"))]
#[doc(hidden)]
pub fn setup_run_encryption(
    _kek: &Kek,
    _indexable_columns: &[(u16, u8)],
) -> crate::Result<RunEncryption> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

/// Unreachable stub — see [`setup_run_encryption`].
#[cfg(not(feature = "encryption"))]
#[doc(hidden)]
pub fn build_run_cipher(_kek: &Kek, _descriptor_bytes: &[u8]) -> crate::Result<RunEncryption> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_is_identity() {
        let c = PlaintextCipher;
        let ct = c.encrypt_page(&[0; 12], b"hello").unwrap();
        assert_eq!(ct, b"hello");
        let pt = c.decrypt_page(&[0; 12], &ct).unwrap();
        assert_eq!(pt, b"hello");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn aes_round_trip() {
        let c = AesCipher::new(&[7u8; 32]).unwrap();
        let nonce = [1u8; 12];
        let ct = c.encrypt_page(&nonce, b"secret page").unwrap();
        assert_ne!(ct, b"secret page");
        let pt = c.decrypt_page(&nonce, &ct).unwrap();
        assert_eq!(pt, b"secret page");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn kek_derive_is_deterministic_for_same_passphrase_and_salt() {
        let salt = random_salt();
        let k1 = Kek::derive("correct horse battery staple", &salt).unwrap();
        let k2 = Kek::derive("correct horse battery staple", &salt).unwrap();
        let dek = generate_dek();
        let np = random_nonce_prefix();
        let w1 = k1.wrap_dek(&dek, &np).unwrap();
        let w2 = k2.wrap_dek(&dek, &np).unwrap();
        assert_eq!(w1, w2, "same passphrase+salt must yield same KEK");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn kek_differs_for_different_salt() {
        let s1 = random_salt();
        let s2 = random_salt();
        let k1 = Kek::derive("passphrase", &s1).unwrap();
        let k2 = Kek::derive("passphrase", &s2).unwrap();
        let dek = generate_dek();
        let np = random_nonce_prefix();
        let w1 = k1.wrap_dek(&dek, &np).unwrap();
        let w2 = k2.wrap_dek(&dek, &np).unwrap();
        assert_ne!(w1, w2, "different salts must yield different KEKs");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn dek_wrap_unwrap_round_trip() {
        let salt = random_salt();
        let kek = Kek::derive("hunter2", &salt).unwrap();
        let dek = generate_dek();
        let np = random_nonce_prefix();
        let wrapped = kek.wrap_dek(&dek, &np).unwrap();
        assert_eq!(wrapped.len(), DEK_LEN + 16);
        let unwrapped = kek.unwrap_dek(&wrapped, &np).unwrap();
        assert_eq!(unwrapped.as_ref(), dek.as_ref());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn unwrap_rejects_wrong_passphrase() {
        let salt = random_salt();
        let enc_kek = Kek::derive("right-pass", &salt).unwrap();
        let dec_kek = Kek::derive("wrong-pass", &salt).unwrap();
        let dek = generate_dek();
        let np = random_nonce_prefix();
        let wrapped = enc_kek.wrap_dek(&dek, &np).unwrap();
        assert!(dec_kek.unwrap_dek(&wrapped, &np).is_err());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn page_nonce_overlays_column_and_page() {
        let np = random_nonce_prefix();
        let n = build_page_nonce(np, 0x0304, 0x0506);
        // high 8 bytes preserved (random), low 4 = column_id (LE) + page_seq (LE).
        assert_eq!(&n[..8], &np[..8]);
        assert_eq!(n[8..10], [0x04, 0x03]);
        assert_eq!(n[10..12], [0x06, 0x05]);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn page_nonce_unique_per_column_and_page() {
        let np = random_nonce_prefix();
        let a = build_page_nonce(np, 1, 0);
        let b = build_page_nonce(np, 1, 1);
        let c = build_page_nonce(np, 2, 0);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn column_key_is_deterministic_from_kek() {
        let salt = random_salt();
        let k1 = Kek::derive("pass", &salt).unwrap();
        let k2 = Kek::derive("pass", &salt).unwrap();
        let c1 = k1.derive_column_key(7);
        let c2 = k2.derive_column_key(7);
        assert_eq!(c1.as_ref(), c2.as_ref(), "same KEK + column => same key");
        // Different columns get different keys.
        let c3 = k1.derive_column_key(8);
        assert_ne!(c1.as_ref(), c3.as_ref());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn hmac_token_collides_only_for_equal_values() {
        let salt = random_salt();
        let k = Kek::derive("pass", &salt).unwrap();
        let ck = k.derive_column_key(1);
        let a = hmac_token(&ck, b"hello");
        let b = hmac_token(&ck, b"hello");
        let c = hmac_token(&ck, b"world");
        assert_eq!(a, b, "equal plaintexts => equal tokens");
        assert_ne!(a, c, "unequal plaintexts => distinct tokens");
        // A different column key yields a different token for the same value.
        let ck2 = k.derive_column_key(2);
        assert_ne!(a, hmac_token(&ck2, b"hello"));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn ope_token_i64_preserves_order() {
        let salt = random_salt();
        let k = Kek::derive("pass", &salt).unwrap();
        let ck = k.derive_column_key(3);
        let vals = [i64::MIN, -1_000_000, -1, 0, 1, 42, 1_000_000, i64::MAX];
        let tokens: Vec<_> = vals.iter().map(|&x| ope_token_i64(&ck, x)).collect();
        // Strictly increasing (big-endian u128 byte order == numeric order).
        for w in tokens.windows(2) {
            assert!(w[0] < w[1], "OPE must preserve order");
        }
        // Equal values map to equal tokens (deterministic).
        assert_eq!(ope_token_i64(&ck, 0), ope_token_i64(&ck, 0));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn ope_token_f64_preserves_order() {
        let salt = random_salt();
        let k = Kek::derive("pass", &salt).unwrap();
        let ck = k.derive_column_key(4);
        let vals = [
            f64::NEG_INFINITY,
            -1.5,
            0.0,
            std::f64::consts::PI,
            1e9,
            f64::INFINITY,
        ];
        let tokens: Vec<_> = vals.iter().map(|&x| ope_token_f64(&ck, x)).collect();
        for w in tokens.windows(2) {
            assert!(w[0] < w[1], "OPE over f64 must preserve total order");
        }
        // Negative < positive (verifies the sign handling).
        assert!(ope_token_f64(&ck, -1.0) < ope_token_f64(&ck, 1.0));
    }

    /// Regression for the Phase 10 review CRITICAL: within one run the DEK and
    /// every column key must be wrapped under DISTINCT nonces, or AES-GCM nonce
    /// reuse under the KEK is catastrophic.
    #[cfg(feature = "encryption")]
    #[test]
    fn wrap_nonces_are_distinct_within_a_run() {
        use super::key::{wrap_nonce, WRAP_KIND_COLUMN, WRAP_KIND_DEK};
        let salt = random_salt();
        let kek = Kek::derive("pass", &salt).unwrap();
        let np = random_nonce_prefix();

        // Distinct nonces for the DEK and each column.
        let dek_n = wrap_nonce(np, WRAP_KIND_DEK, 0);
        let col1 = wrap_nonce(np, WRAP_KIND_COLUMN, 1);
        let col2 = wrap_nonce(np, WRAP_KIND_COLUMN, 2);
        assert_ne!(dek_n, col1);
        assert_ne!(dek_n, col2);
        assert_ne!(col1, col2);

        // Wrapping the SAME key material under the DEK vs a column nonce yields
        // distinct ciphertext — the (KEK, nonce) pair is never reused.
        let k = generate_dek();
        let w_dek = kek.wrap_dek(&k, &dek_n).unwrap();
        let w_col = kek.wrap_column_key(&k, &col1).unwrap();
        assert_ne!(
            w_dek, w_col,
            "DEK and column-key wraps must not share a nonce"
        );

        // End-to-end: setup + build_run_cipher unwrap the DEK consistently.
        let enc =
            setup_run_encryption(&kek, &[(1, SCHEME_HMAC_EQ), (2, SCHEME_OPE_RANGE)]).unwrap();
        let built = build_run_cipher(&kek, &enc.descriptor_bytes).unwrap();
        assert_eq!(built.nonce_prefix, enc.nonce_prefix);
    }
}
