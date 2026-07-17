//! Integration tests for the versioned command envelope (spec 9.3, FND-003).

use mongreldb_log::envelope::{
    CommandEnvelope, EnvelopeError, CHECKSUM_LEN, COMMAND_ENVELOPE_FORMAT_VERSION, HEADER_LEN,
    MAX_COMMAND_PAYLOAD_BYTES,
};

/// xorshift64: a tiny deterministic RNG so these tests need no extra deps.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Value in `0..n` (test data only; modulo bias is fine here).
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

#[test]
fn randomized_payload_round_trips() {
    let mut rng = XorShift::new(0x5EED);
    for _ in 0..256 {
        let command_type = rng.next_u64() as u32;
        let mut command_id = [0u8; 16];
        rng.fill(&mut command_id);
        let mut payload = vec![0u8; rng.below(4097)];
        rng.fill(&mut payload);

        let envelope = CommandEnvelope::new(command_type, command_id, payload);
        envelope.verify().unwrap();
        let bytes = envelope.encode();
        // Encoding is deterministic: re-encoding yields identical bytes.
        assert_eq!(envelope.encode(), bytes);
        let decoded = CommandEnvelope::decode(&bytes).unwrap();
        assert_eq!(decoded, envelope);
        // decode ∘ encode is the identity on canonical bytes.
        assert_eq!(decoded.encode(), bytes);
    }
}

#[test]
fn bit_flips_are_detected() {
    let mut rng = XorShift::new(0xB17);
    for _ in 0..128 {
        let mut command_id = [0u8; 16];
        rng.fill(&mut command_id);
        let mut payload = vec![0u8; 1 + rng.below(512)];
        rng.fill(&mut payload);
        let envelope = CommandEnvelope::new(rng.next_u64() as u32, command_id, payload);
        let mut bytes = envelope.encode();
        // Flip one bit anywhere in the payload or the trailing checksum: both
        // regions are covered by the checksum. (The command id in the header
        // is not covered, per the frozen checksum contract.)
        let flip_at = HEADER_LEN + rng.below(bytes.len() - HEADER_LEN);
        bytes[flip_at] ^= 1u8 << rng.below(8);
        assert_eq!(
            CommandEnvelope::decode(&bytes),
            Err(EnvelopeError::ChecksumMismatch)
        );
    }
}

#[test]
fn unsupported_versions_fail_closed() {
    let mut rng = XorShift::new(0x0BAD);
    for version in [0, COMMAND_ENVELOPE_FORMAT_VERSION + 1, u32::MAX] {
        let mut command_id = [0u8; 16];
        rng.fill(&mut command_id);
        let mut payload = vec![0u8; 32];
        rng.fill(&mut payload);
        // A well-formed frame (valid checksum) whose version is unsupported.
        let envelope = CommandEnvelope {
            format_version: version,
            command_id,
            command_type: 7,
            payload_sha256: CommandEnvelope::checksum(version, 7, &payload),
            payload,
        };
        let bytes = envelope.encode();
        assert!(
            matches!(
                CommandEnvelope::decode(&bytes),
                Err(EnvelopeError::UnsupportedVersion { found, .. }) if found == version
            ),
            "version {version} must fail closed"
        );
    }
}

#[test]
fn truncated_frames_fail_closed() {
    let mut rng = XorShift::new(0xCA7);
    let mut command_id = [0u8; 16];
    rng.fill(&mut command_id);
    let mut payload = vec![0u8; 256];
    rng.fill(&mut payload);
    let envelope = CommandEnvelope::new(3, command_id, payload);
    let bytes = envelope.encode();
    // Every strict prefix of a canonical frame is a truncation error.
    for cut in [
        0,
        1,
        HEADER_LEN - 1,
        HEADER_LEN,
        HEADER_LEN + 1,
        bytes.len() - CHECKSUM_LEN,
        bytes.len() - 1,
    ] {
        assert!(
            matches!(
                CommandEnvelope::decode(&bytes[..cut]),
                Err(EnvelopeError::Truncated { .. })
            ),
            "cut at {cut} must fail closed"
        );
    }
    for _ in 0..64 {
        let cut = rng.below(bytes.len());
        assert!(matches!(
            CommandEnvelope::decode(&bytes[..cut]),
            Err(EnvelopeError::Truncated { .. })
        ));
    }
}

#[test]
fn trailing_bytes_fail_closed() {
    let mut rng = XorShift::new(0x7A11);
    let envelope = CommandEnvelope::new(9, [5u8; 16], b"payload".to_vec());
    let bytes = envelope.encode();
    for extra in 1..=3usize {
        let mut longer = bytes.clone();
        let mut junk = vec![0u8; extra];
        rng.fill(&mut junk);
        longer.extend_from_slice(&junk);
        assert_eq!(
            CommandEnvelope::decode(&longer),
            Err(EnvelopeError::TrailingBytes(extra))
        );
    }
}

#[test]
fn oversize_payloads_fail_closed() {
    // verify() rejects an oversize payload before any checksum work.
    let envelope = CommandEnvelope {
        format_version: COMMAND_ENVELOPE_FORMAT_VERSION,
        command_id: [0u8; 16],
        command_type: 1,
        payload: vec![0u8; MAX_COMMAND_PAYLOAD_BYTES + 1],
        payload_sha256: [0u8; 32],
    };
    assert_eq!(
        envelope.verify(),
        Err(EnvelopeError::PayloadTooLarge(
            MAX_COMMAND_PAYLOAD_BYTES + 1
        ))
    );

    // decode() rejects an oversize length prefix without reading payload bytes.
    let mut frame = Vec::with_capacity(HEADER_LEN + CHECKSUM_LEN);
    frame.extend_from_slice(&COMMAND_ENVELOPE_FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&[0u8; 16]);
    frame.extend_from_slice(&7u32.to_le_bytes());
    frame.extend_from_slice(&((MAX_COMMAND_PAYLOAD_BYTES + 1) as u32).to_le_bytes());
    frame.extend_from_slice(&[0u8; CHECKSUM_LEN]);
    assert_eq!(
        CommandEnvelope::decode(&frame),
        Err(EnvelopeError::PayloadTooLarge(
            MAX_COMMAND_PAYLOAD_BYTES + 1
        ))
    );
}

#[test]
fn encode_decode_identity() {
    let envelope = CommandEnvelope::new(42, [0xAB; 16], b"identity".to_vec());
    let bytes = envelope.encode();
    assert_eq!(CommandEnvelope::decode(&bytes).unwrap().encode(), bytes);
}
