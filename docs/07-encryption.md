# Encryption

MongrelDB can encrypt data at rest using AES-256-GCM. When encryption is
enabled, the contents of your sorted-run files (`.sr`) are unreadable without
the passphrase. This protects sensitive data if someone gains access to the
storage disk.

## The Short Version

You control encryption with a **passphrase** — a string you choose. There's no
key file to manage, no environment variable to set, no cloud KMS to configure.
If you have the passphrase, you can read the data. If you don't, you can't.

```rust
// Create an encrypted database
let db = Db::create_encrypted("./mydb", schema, 1, "my-secret-passphrase")?;

// Reopen it later (same passphrase)
let db = Db::open_encrypted("./mydb", "my-secret-passphrase")?;
```

That's it. Everything else — key derivation, per-page encryption, key wrapping
— happens automatically.

## Enabling the Feature

Encryption is behind a Cargo feature flag. Add it to your `Cargo.toml`:

```toml
[dependencies]
mongreldb-core = { path = "...", features = ["encryption"] }
```

Without the `encryption` feature, `create_encrypted` and `open_encrypted` are
not available.

## What Actually Happens Behind the Scenes

When you create an encrypted database, MongrelDB sets up a multi-layer key
system. You don't need to understand this to use encryption, but it helps to
know why it's secure:

1. **Passphrase → KEK.** Your passphrase is run through Argon2id (a
   memory-hard key derivation function that's deliberately slow — about 19 MB
   of memory and 2 iterations). This produces a 256-bit Key-Encryption Key
   (KEK). A random 16-byte salt is stored on disk (the salt is not secret —
   its purpose is to ensure different databases with the same passphrase
   produce different keys).

2. **KEK → DEK.** Each sorted-run file gets its own random 256-bit Data
   Encryption Key (DEK). The DEK is what actually encrypts the page data.
   The DEK is stored inside the run file, but wrapped (encrypted) by the KEK.

3. **KEK → Column keys.** For columns marked `ENCRYPTED_INDEXABLE`, a
   per-column key is derived from the KEK. These keys allow the column's
   values to be transformed into tokens that can be indexed (for equality
   search) without revealing the plaintext. This uses HMAC for equality
   tokens and order-preserving encryption (OPE) for range queries.

All keys in memory are held in `Zeroizing` wrappers — they're overwritten
with zeros when no longer needed.

## What Gets Encrypted

| Storage component | Encrypted? | Notes |
|---|---|---|
| Sorted-run page data (`.sr`) | **Yes** | Each page encrypted independently with AES-256-GCM |
| Sorted-run headers | No | Structural metadata needed to open the file |
| WAL segments (`_wal/`) | **Yes** (encrypted tables) | Frame-level AES-256-GCM when the table is encrypted |
| Manifest, schema, index files | No | Non-data metadata |
| Result cache (`_rcache/`) | **Yes** (encrypted tables) | AES-256-GCM encrypted cache files |

### Key Files vs Passphrases

MongrelDB supports two ways to provide the encryption key:

**Passphrase** (human-memorable, slow derivation):
```rust
let db = Db::create_encrypted(dir, schema, 1, "my-secret-passphrase")?;
let db = Db::open_encrypted(dir, "my-secret-passphrase")?;
```
Uses Argon2id (~50ms) to stretch the passphrase into a strong key.

**Raw key** (machine-generated, fast derivation):
```rust
let key = std::fs::read("my.key")?;  // 32+ bytes of random data
let db = Db::create_with_key(dir, schema, 1, &key)?;
let db = Db::open_with_key(dir, &key)?;
```
Skips Argon2id — uses HKDF-SHA256 only (~0.1ms). The key must already be
high-entropy (generate one with `openssl rand 32 > my.key`).

Both paths produce the same KEK; all downstream encryption (sorted runs, WAL,
cache) is identical regardless of which method you used.

**Note:** For encrypted tables, the WAL is also encrypted (frame-level
AES-256-GCM). For plaintext tables, the WAL stores rows unencrypted.

## Encrypted Indexable Columns

If you want to search encrypted columns (equality or range), mark them
`ENCRYPTED_INDEXABLE`:

```rust
ColumnDef {
    id: 2,
    name: "ssn".into(),
    ty: TypeId::Bytes,
    flags: ColumnFlags::empty()
        .with(ColumnFlags::ENCRYPTED_INDEXABLE),
}
```

MongrelDB will:
- Store the encrypted value in the page data (AES-256-GCM)
- Also store a deterministic token (HMAC or OPE) for the index
- The bitmap/HOT indexes use the token, so they work without decrypting

This means `Condition::BitmapEq { column_id: 2, value: ... }` still works
on encrypted columns — the value is tokenized the same way before lookup.

## Performance

AES-256-GCM runs at about **1.87 GiB/s** with hardware acceleration (AES-NI).
In practice, encryption adds less than 5% overhead to bulk operations:

| Operation | Plain | Encrypted | Overhead |
|---|---|---|---|
| Bulk ingest (1M rows) | 194 ms | 149 ms | negligible |
| Cold SQL filter | 7.2 ms | 7.7 ms | ~7% |
| SQL join | 1.55 ms | 1.45 ms | negligible |

(The encrypted path is sometimes faster due to differences in run layout —
this is within measurement noise.)

## Losing the Passphrase

If you lose the passphrase, the data is unrecoverable. There is no back door,
no recovery key, no master override. The KEK cannot be reconstructed without
the passphrase, and the DEKs cannot be unwrapped without the KEK.

Store your passphrase securely — a password manager, a secrets service, or
wherever you store other critical credentials.
