#![no_main]

use libfuzzer_sys::fuzz_target;
use mongreldb_protocol::envelope::ProtocolEnvelope;

fuzz_target!(|data: &[u8]| {
    let _ = ProtocolEnvelope::decode(data);
});
