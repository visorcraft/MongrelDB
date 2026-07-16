use crate::cstr::string_into_raw;
use std::ffi::c_char;

#[no_mangle]
pub extern "C" fn mongreldb_build_info() -> *mut c_char {
    let query = mongreldb_query::build_info();
    string_into_raw(
        serde_json::json!({
            "artifact_version": env!("CARGO_PKG_VERSION"),
            "engine_version": query.engine_version,
            "query_version": query.query_version,
            "mongreldb_git_sha": query.mongreldb_git_sha,
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn ffi_build_info_matches_component_train() {
        let pointer = super::mongreldb_build_info();
        let json = unsafe { std::ffi::CStr::from_ptr(pointer) }
            .to_str()
            .unwrap();
        let info: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(info["artifact_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(info["engine_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(info["query_version"], env!("CARGO_PKG_VERSION"));
        unsafe { crate::mongreldb_free_string(pointer) };
    }
}
