#![no_main]

use libfuzzer_sys::fuzz_target;
use mongreldb_log::CommandEnvelope;

fuzz_target!(|data: &[u8]| {
    let _ = CommandEnvelope::decode(data);
});
