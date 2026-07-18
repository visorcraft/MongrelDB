fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/common.proto",
        "proto/auth.proto",
        "proto/session.proto",
        "proto/query.proto",
        "proto/transaction.proto",
        "proto/catalog.proto",
        "proto/admin.proto",
        "proto/health.proto",
    ];
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&protos, &["proto"])?;
    Ok(())
}
