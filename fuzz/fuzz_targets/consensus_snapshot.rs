#![no_main]

use libfuzzer_sys::fuzz_target;
use mongreldb_core::EngineSnapshot;

fuzz_target!(|data: &[u8]| {
    let _ = EngineSnapshot::decode(data);
});
