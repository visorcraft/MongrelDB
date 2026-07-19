use std::net::SocketAddr;
use std::time::Duration;

use mongreldb_client::native::NativeClient;
use mongreldb_mysql_wire::{serve, MysqlWireConfig};
use mongreldb_protocol::native_transport::NativeRpcClientConfig;

fn value(args: &[String], name: &str) -> Result<String, String> {
    let index = args
        .iter()
        .position(|argument| argument == name)
        .ok_or_else(|| format!("{name} is required"))?;
    args.get(index + 1)
        .cloned()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn database_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err("--database-id must be 32 hexadecimal characters".into());
    }
    let mut output = [0_u8; 16];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "--database-id must be 32 hexadecimal characters")?;
    }
    Ok(output)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let listen: SocketAddr = value(&args, "--listen")?
        .parse()
        .map_err(|_| "--listen must be an IP socket address")?;
    let certificate_pem =
        std::fs::read(value(&args, "--tls-cert")?).map_err(|error| error.to_string())?;
    let private_key_pem =
        std::fs::read(value(&args, "--tls-key")?).map_err(|error| error.to_string())?;
    let native_ca =
        std::fs::read(value(&args, "--native-ca")?).map_err(|error| error.to_string())?;
    let database_id = database_id(&value(&args, "--database-id")?)?;
    let client = NativeClient::connect(
        NativeRpcClientConfig {
            endpoint: value(&args, "--native-endpoint")?,
            domain_name: value(&args, "--native-domain")?,
            ca_certificate_pem: native_ca,
            client_identity_pem: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_in_flight: 1_024,
            tcp_keepalive: Duration::from_secs(30),
            http2_keepalive_interval: Duration::from_secs(30),
        },
        4,
        database_id,
    )
    .await
    .map_err(|error| error.to_string())?;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| error.to_string())?;
    serve(
        listener,
        MysqlWireConfig {
            certificate_pem,
            private_key_pem,
            database_name: value(&args, "--database-name")?,
            max_connections: 1_024,
            handshake_timeout: Duration::from_secs(10),
        },
        client,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
    .map_err(|error| error.to_string())
}
