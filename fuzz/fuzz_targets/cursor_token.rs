#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(value) = std::str::from_utf8(data) {
        mongreldb_server::fuzz_validate_sql_cursor(value, "fuzz-owner", &[7; 32]);
    }
});
