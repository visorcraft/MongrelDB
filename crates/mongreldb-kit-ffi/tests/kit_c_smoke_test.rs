//! Compiles and runs the C smoke test against libmongreldb_kit.

use std::path::PathBuf;

#[test]
fn kit_c_smoke_test() {
    use std::process::Command;

    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let header = crate_root.join("include/mongreldb_kit.h");
    let c_source = crate_root.join("tests/kit_c_test.c");
    let test_binary = std::env::current_exe().expect("failed to locate test binary");
    let lib_path = test_binary
        .parent()
        .and_then(|deps| deps.parent())
        .expect("test binary is not under target/release/deps");

    assert!(header.exists(), "mongreldb_kit.h not found at {:?}", header);
    assert!(
        c_source.exists(),
        "kit_c_test.c not found at {:?}",
        c_source
    );
    assert!(
        lib_path.join("libmongreldb_kit.so").exists()
            || lib_path.join("libmongreldb_kit.a").exists()
            || lib_path.join("libmongreldb_kit.dylib").exists()
            || lib_path.join("mongreldb_kit.dll").exists()
            || lib_path.join("libmongreldb_kit.dll").exists()
            || lib_path.join("libmongreldb_kit.dll.a").exists(),
        "libmongreldb_kit not found under {} (CARGO_TARGET_DIR={:?})",
        lib_path.display(),
        std::env::var_os("CARGO_TARGET_DIR")
    );

    // Compile the C test, linking against the shared library.
    let output = Command::new("cc")
        .args([
            "-o",
            "/tmp/mongreldb_kit_c_smoke",
            c_source.to_str().unwrap(),
            "-I",
            header.parent().unwrap().to_str().unwrap(),
            "-L",
            lib_path.to_str().unwrap(),
            "-lmongreldb_kit",
            "-Wl,-rpath",
            lib_path.to_str().unwrap(),
            "-lpthread",
            "-ldl",
            "-lm",
        ])
        .output()
        .expect("failed to invoke cc");

    if !output.status.success() {
        panic!(
            "C compilation failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Run the compiled C test.
    let output = Command::new("/tmp/mongreldb_kit_c_smoke")
        .output()
        .expect("failed to run C test binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("Kit C test stdout:\n{}", stdout);
    if !stderr.is_empty() {
        eprintln!("Kit C test stderr:\n{}", stderr);
    }

    assert!(
        output.status.success(),
        "Kit C smoke test exited with status {:?}",
        output.status
    );
    assert!(
        stdout.contains("All Kit C smoke tests passed!"),
        "Kit C smoke test did not print success message"
    );
}
