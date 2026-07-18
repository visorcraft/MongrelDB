//! Compiles and runs the C smoke test against libmongreldb.

#[test]
fn c_smoke_test() {
    use std::path::PathBuf;
    use std::process::Command;

    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let header = crate_root.join("include/mongreldb.h");
    let c_source = crate_root.join("tests/c_test.c");
    // Respect CARGO_TARGET_DIR (CI Clean release qualification sets this to
    // /tmp/mongreldb-qualification-target). Without it, link looks under
    // crates/mongreldb-ffi/target/release and fails with -lmongreldb missing.
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate_root.join("target"));
    let lib_path = target_dir.join("release");
    let sanitize = std::env::var_os("MONGRELDB_C_SANITIZE").is_some();
    let binary = if sanitize {
        "/tmp/mongreldb_c_smoke_sanitize"
    } else {
        "/tmp/mongreldb_c_smoke"
    };

    assert!(header.exists(), "mongreldb.h not found at {:?}", header);
    assert!(c_source.exists(), "c_test.c not found at {:?}", c_source);

    let status = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&crate_root)
        .status()
        .expect("failed to build C library");
    assert!(status.success(), "failed to build C library");

    assert!(
        lib_path.join("libmongreldb.so").exists()
            || lib_path.join("libmongreldb.a").exists()
            || lib_path.join("libmongreldb.dylib").exists()
            || lib_path.join("mongreldb.dll").exists()
            || lib_path.join("libmongreldb.dll").exists()
            || lib_path.join("libmongreldb.dll.a").exists(),
        "libmongreldb not found under {} after cargo build --release (CARGO_TARGET_DIR={:?})",
        lib_path.display(),
        std::env::var_os("CARGO_TARGET_DIR")
    );

    // Compile the C test, linking against the shared library.
    let mut compiler = Command::new("cc");
    compiler.args([
        "-std=c11",
        "-D_GNU_SOURCE",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-o",
        binary,
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
    ]);
    if sanitize {
        compiler.args(["-fsanitize=address,undefined", "-fno-omit-frame-pointer"]);
    }
    let output = compiler.output().expect("failed to invoke cc");

    if !output.status.success() {
        panic!(
            "C compilation failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Run the compiled C test.
    let mut smoke = Command::new(binary);
    if sanitize {
        smoke.env("ASAN_OPTIONS", "detect_leaks=0:halt_on_error=1");
        smoke.env("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1");
    }
    let output = smoke.output().expect("failed to run C test binary");

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
