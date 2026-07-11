//! Compiles and runs the C smoke test against libmongreldb.

#[test]
fn c_smoke_test() {
    use std::path::PathBuf;
    use std::process::Command;

    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let header = crate_root.join("include/mongreldb.h");
    let c_source = crate_root.join("tests/c_test.c");
    let lib_path = crate_root.join("target/release");

    assert!(header.exists(), "mongreldb.h not found at {:?}", header);
    assert!(c_source.exists(), "c_test.c not found at {:?}", c_source);

    // Compile the C test, linking against the shared library.
    let output = Command::new("cc")
        .args([
            "-o",
            "/tmp/mongreldb_c_smoke",
            c_source.to_str().unwrap(),
            "-I",
            header.parent().unwrap().to_str().unwrap(),
            "-L",
            lib_path.to_str().unwrap(),
            "-lmongreldb",
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
    let output = Command::new("/tmp/mongreldb_c_smoke")
        .output()
        .expect("failed to run C test binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("C test stdout:\n{}", stdout);
    if !stderr.is_empty() {
        eprintln!("C test stderr:\n{}", stderr);
    }

    assert!(
        output.status.success(),
        "C smoke test exited with status {:?}",
        output.status
    );
    assert!(
        stdout.contains("All C smoke tests passed!"),
        "C smoke test did not print success message"
    );
}
