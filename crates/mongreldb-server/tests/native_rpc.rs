use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::ipc::reader::StreamReader;
use axum::body::{to_bytes, Body};
use axum::http::Request;
use mongreldb_client::native::NativeClient;
use mongreldb_core::constraint::{TableConstraints, UniqueConstraint};
use mongreldb_core::embedding::EmbeddingSource;
use mongreldb_core::schema::{
    AnnOptions, AnnQuantization, IndexDef, IndexKind, IndexOptions, LearnedRangeOptions,
    MinHashOptions,
};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_protocol::native;
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcConnection, NativeRpcServer, NativeRpcServerConfig,
    NativeRpcServices,
};
use mongreldb_protocol::NATIVE_API_MAJOR;
use mongreldb_server::native::NativeExternalAuth;
use mongreldb_server::{build_app_with_sessions_and_control, SessionStore};
use prost::Message;
use serde_json::json;
use tempfile::tempdir;
use tower::ServiceExt;

fn context(deadline_unix_micros: u64) -> Option<native::RequestContext> {
    Some(native::RequestContext {
        version: Some(native::ApiVersion {
            major: NATIVE_API_MAJOR,
            minor: 0,
        }),
        request_id: "test".into(),
        deadline_unix_micros,
        idempotency_key: String::new(),
    })
}

fn idempotent_context(key: &str) -> Option<native::RequestContext> {
    let mut context = context(0).unwrap();
    context.idempotency_key = key.into();
    Some(context)
}

#[tokio::test]
async fn native_runtime_serves_all_services_over_tls_http2() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_user("native-user", "native-password").unwrap();
    let db = Arc::new(db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (app, control) = build_app_with_sessions_and_control(
        Arc::clone(&db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let external_auth = NativeExternalAuth::new();
    external_auth
        .upsert_service_token(
            mongreldb_core::ServiceToken::mint(
                "native-token",
                "native-user",
                vec!["query".into()],
                "native-token-secret",
                0,
            )
            .unwrap(),
        )
        .unwrap();
    let runtime = control
        .native_runtime(Arc::clone(&db), Arc::clone(&sessions))
        .with_external_auth(external_auth);
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certified.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 32,
            max_concurrent_streams: 32,
            max_in_flight_per_connection: 32,
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(5),
        })
        .serve_with_shutdown(
            NativeRpcServices {
                auth: runtime.clone(),
                session: runtime.clone(),
                query: runtime.clone(),
                transaction: runtime.clone(),
                catalog: runtime.clone(),
                admin: runtime.clone(),
                health: runtime,
            },
            async move {
                let _ = shutdown_rx.await;
            },
        ),
    );
    let config = NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{}", address.port()),
        domain_name: "localhost".into(),
        ca_certificate_pem: certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(30),
        max_in_flight: 32,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let mut connection = None;
    for _ in 0..100 {
        match NativeRpcConnection::connect(&config).await {
            Ok(connected) => {
                connection = Some(connected);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    let connection = connection.expect("native listener did not start");
    let client = connection.client();

    let auth = client
        .auth()
        .authenticate(native::AuthenticateRequest {
            context: context(0),
            credential: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(auth.auth_token.len(), 16);
    let session_id = client
        .session()
        .open_session(native::OpenSessionRequest {
            context: context(0),
            identity: auth.identity,
            database_id: sessions.database_id().as_bytes().to_vec(),
            auth_token: auth.auth_token,
        })
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let service_auth = client
        .auth()
        .authenticate(native::AuthenticateRequest {
            context: context(0),
            credential: Some(native::authenticate_request::Credential::ServiceToken(
                native::ServiceTokenCredential {
                    token_id: "native-token".into(),
                    secret: "native-token-secret".into(),
                },
            )),
        })
        .await
        .unwrap()
        .into_inner();
    let service_session_id = client
        .session()
        .open_session(native::OpenSessionRequest {
            context: context(0),
            identity: service_auth.identity,
            database_id: sessions.database_id().as_bytes().to_vec(),
            auth_token: service_auth.auth_token,
        })
        .await
        .unwrap()
        .into_inner()
        .session_id;
    assert!(client
        .admin()
        .execute_admin(native::ExecuteAdminRequest {
            context: context(0),
            session_id: service_session_id.clone(),
            command: b"CREATE TABLE denied (id BIGINT)".to_vec(),
        })
        .await
        .is_err());

    client
        .admin()
        .execute_admin(native::ExecuteAdminRequest {
            context: context(0),
            session_id: session_id.clone(),
            command: b"CREATE TABLE native_items (id BIGINT PRIMARY KEY)".to_vec(),
        })
        .await
        .unwrap();
    let schema = client
        .catalog()
        .get_schema(native::GetSchemaRequest {
            context: context(0),
            database_id: sessions.database_id().as_bytes().to_vec(),
            table: "native_items".into(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(schema.columns.len(), 1);

    let prepared = client
        .query()
        .prepare(native::PrepareRequest {
            context: context(0),
            session_id: session_id.clone(),
            sql: "SELECT 42 AS answer".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let response = client
        .query()
        .execute(native::ExecuteRequest {
            context: context(0),
            session_id: session_id.clone(),
            query_id: vec![7; 16],
            command: Some(native::execute_request::Command::PreparedStatementId(
                prepared.statement_id,
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let ipc = &response
        .frames
        .iter()
        .find(|frame| !frame.ipc.is_empty())
        .unwrap()
        .ipc;
    let batches = StreamReader::try_new(Cursor::new(ipc), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let native_answer = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let http = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "sql": "SELECT 42 AS answer" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(http.status(), 200);
    let http: serde_json::Value =
        serde_json::from_slice(&to_bytes(http.into_body(), 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!(http[0]["answer"], native_answer, "{http}");

    let mut concurrent = tokio::task::JoinSet::new();
    for value in 0_u8..8 {
        let client = client.clone();
        let session_id = session_id.clone();
        concurrent.spawn(async move {
            client
                .query()
                .execute(native::ExecuteRequest {
                    context: context(0),
                    session_id,
                    query_id: vec![value + 32; 16],
                    command: Some(native::execute_request::Command::Sql(format!(
                        "SELECT {value} AS value"
                    ))),
                    parameters: Vec::new(),
                })
                .await
        });
    }
    while let Some(result) = concurrent.join_next().await {
        assert!(result.unwrap().is_ok());
    }

    let first = client
        .query()
        .execute(native::ExecuteRequest {
            context: idempotent_context("native-insert-1"),
            session_id: session_id.clone(),
            query_id: vec![8; 16],
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO native_items VALUES (1)".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(first.committed);
    assert!(!first.idempotency_replayed);
    let replay = client
        .query()
        .execute(native::ExecuteRequest {
            context: idempotent_context("native-insert-1"),
            session_id: session_id.clone(),
            query_id: vec![9; 16],
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO native_items VALUES (1)".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(replay.committed);
    assert!(replay.idempotency_replayed);

    let transaction = client
        .transaction()
        .begin(native::BeginTransactionRequest {
            context: context(0),
            session_id: session_id.clone(),
            isolation: native::IsolationLevel::Serializable as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(transaction.transaction_id.len(), 16);
    client
        .transaction()
        .rollback(native::TransactionRequest {
            context: context(0),
            session_id: session_id.clone(),
        })
        .await
        .unwrap();

    assert!(
        client
            .health()
            .status(native::HealthRequest {
                context: context(0)
            })
            .await
            .unwrap()
            .into_inner()
            .serving
    );

    let high_level = NativeClient::connect(config.clone(), 2, *sessions.database_id().as_bytes())
        .await
        .unwrap()
        .authenticate_anonymous()
        .await
        .unwrap();
    high_level
        .create_table(
            "native_unique_items",
            &Schema {
                schema_id: 0,
                columns: vec![
                    ColumnDef {
                        id: 0,
                        name: "id".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 1,
                        name: "email".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: TableConstraints {
                    uniques: vec![UniqueConstraint {
                        id: 1,
                        name: "uq_native_email".into(),
                        columns: vec![1],
                    }],
                    foreign_keys: Vec::new(),
                    checks: Vec::new(),
                },
                clustered: false,
            },
        )
        .await
        .unwrap();

    let full_schema = Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 0,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 1,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 3 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: Some(EmbeddingSource::ConfiguredModel {
                    provider_id: "native-provider".into(),
                    model_id: "dense-model".into(),
                    model_version: "1".into(),
                }),
            },
            ColumnDef {
                id: 3,
                name: "set".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "by_id".into(),
                column_id: 0,
                kind: IndexKind::Bitmap,
                predicate: Some("id > 0".into()),
                options: IndexOptions::default(),
            },
            IndexDef {
                name: "by_text".into(),
                column_id: 1,
                kind: IndexKind::FmIndex,
                predicate: None,
                options: IndexOptions::default(),
            },
            IndexDef {
                name: "by_embedding".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: IndexOptions {
                    ann: Some(AnnOptions {
                        m: 24,
                        ef_construction: 96,
                        ef_search: 48,
                        quantization: AnnQuantization::Dense,
                        ..AnnOptions::default()
                    }),
                    ..IndexOptions::default()
                },
            },
            IndexDef {
                name: "by_range".into(),
                column_id: 0,
                kind: IndexKind::LearnedRange,
                predicate: None,
                options: IndexOptions {
                    learned_range: Some(LearnedRangeOptions { epsilon: 8 }),
                    ..IndexOptions::default()
                },
            },
            IndexDef {
                name: "by_set".into(),
                column_id: 3,
                kind: IndexKind::MinHash,
                predicate: None,
                options: IndexOptions {
                    minhash: Some(MinHashOptions {
                        permutations: 64,
                        bands: 16,
                    }),
                    ..IndexOptions::default()
                },
            },
            IndexDef {
                name: "by_sparse".into(),
                column_id: 4,
                kind: IndexKind::Sparse,
                predicate: None,
                options: IndexOptions::default(),
            },
        ],
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    };
    high_level
        .create_table("native_ai_documents", &full_schema)
        .await
        .unwrap();
    let returned = high_level.schema("native_ai_documents").await.unwrap();
    assert_eq!(returned.indexes, full_schema.indexes);
    assert_eq!(
        returned.columns[2].embedding_source,
        full_schema.columns[2].embedding_source
    );
    let created = db.table("native_ai_documents").unwrap();
    let created = created.lock().schema().clone();
    assert_eq!(created.indexes, full_schema.indexes);
    assert_eq!(
        created.columns[2].embedding_source,
        full_schema.columns[2].embedding_source
    );
    high_level
        .execute(
            "INSERT INTO native_unique_items VALUES (1, 'same@example.test')",
            Some("native-unique-first"),
        )
        .await
        .unwrap();
    assert!(high_level
        .execute(
            "INSERT INTO native_unique_items VALUES (2, 'same@example.test')",
            Some("native-unique-second"),
        )
        .await
        .is_err());
    let mut stream = high_level
        .execute_stream("SELECT 7 AS value")
        .await
        .unwrap();
    assert_eq!(stream.next_batch().await.unwrap().unwrap().num_rows(), 1);
    assert!(stream.next_batch().await.unwrap().is_none());
    let bulk_values = (2..=20_000)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(",");
    high_level
        .execute(
            format!("INSERT INTO native_items VALUES {bulk_values}"),
            Some("native-bulk"),
        )
        .await
        .unwrap();
    let cancelled = high_level
        .execute_stream(
            "SELECT left_side.id FROM native_items left_side CROSS JOIN native_items right_side",
        )
        .await
        .unwrap();
    high_level.cancel(cancelled.query_id()).await.unwrap();
    drop(cancelled);
    let mut unaffected = high_level
        .execute_stream("SELECT 9 AS unaffected")
        .await
        .unwrap();
    assert_eq!(
        unaffected.next_batch().await.unwrap().unwrap().num_rows(),
        1
    );

    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .saturating_add(Duration::from_secs(5))
        .as_micros() as u64;
    let mut deadline_stream = client
        .query()
        .execute_stream(native::ExecuteRequest {
            context: context(deadline),
            session_id: session_id.clone(),
            query_id: vec![88; 16],
            command: Some(native::execute_request::Command::Sql(
                "SELECT left_side.id FROM native_items left_side CROSS JOIN native_items right_side"
                    .into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(5_200)).await;
    let deadline_error = loop {
        match tokio::time::timeout(Duration::from_secs(5), deadline_stream.message())
            .await
            .expect("deadline stream stalled")
        {
            Ok(Some(_)) => {}
            Ok(None) => panic!("deadline stream completed cleanly"),
            Err(error) => break error,
        }
    };
    assert_eq!(deadline_error.code(), tonic::Code::DeadlineExceeded);
    let detail = native::ErrorDetail::decode(deadline_error.details()).unwrap();
    assert_eq!(detail.category, "deadline exceeded");
    high_level.close().await.unwrap();

    let password_session =
        NativeClient::connect(config.clone(), 1, *sessions.database_id().as_bytes())
            .await
            .unwrap()
            .authenticate_password(
                "native-user",
                &mongreldb_client::SecretString::new("native-password".into()),
            )
            .await
            .unwrap();
    password_session.close().await.unwrap();
    assert!(
        NativeClient::connect(config, 1, *sessions.database_id().as_bytes())
            .await
            .unwrap()
            .authenticate_password(
                "native-user",
                &mongreldb_client::SecretString::new("wrong-password".into()),
            )
            .await
            .is_err()
    );

    client
        .session()
        .close_session(native::CloseSessionRequest {
            context: context(0),
            session_id,
        })
        .await
        .unwrap();
    client
        .session()
        .close_session(native::CloseSessionRequest {
            context: context(0),
            session_id: service_session_id,
        })
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
