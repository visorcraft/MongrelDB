use std::ffi::{c_char, CString};

#[no_mangle]
pub extern "C" fn mongreldb_kit_build_info() -> *mut c_char {
    let info = serde_json::json!({
        "artifact_version": env!("CARGO_PKG_VERSION"),
        "engine_version": env!("CARGO_PKG_VERSION"),
        "query_version": env!("CARGO_PKG_VERSION"),
        "kit_version": env!("CARGO_PKG_VERSION"),
        "mongreldb_git_sha": env!("MONGRELDB_GIT_SHA"),
        "kit_git_sha": env!("MONGRELDB_KIT_GIT_SHA"),
    });
    CString::new(serde_json::to_string(&info).expect("build info is serializable"))
        .expect("build info contains no NUL")
        .into_raw()
}

#[cfg(test)]
mod tests {
    #[test]
    fn kit_ffi_build_info_matches_component_train() {
        let pointer = super::mongreldb_kit_build_info();
        let json = unsafe { std::ffi::CStr::from_ptr(pointer) }
            .to_str()
            .unwrap();
        let info: serde_json::Value = serde_json::from_str(json).unwrap();
        for field in [
            "artifact_version",
            "engine_version",
            "query_version",
            "kit_version",
        ] {
            assert_eq!(info[field], env!("CARGO_PKG_VERSION"));
        }
        unsafe { crate::mongreldb_kit_free_json(pointer) };
    }
}
