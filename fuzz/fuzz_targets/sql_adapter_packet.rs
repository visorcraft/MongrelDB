#![no_main]

use libfuzzer_sys::fuzz_target;
use mongreldb_protocol::native::ExecuteRequest;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let _ = ExecuteRequest::decode(data);
});
