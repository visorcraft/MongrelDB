use mongreldb_client::{ClientError, KitErrorCode, MongrelClient};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn blocking_legacy_write_responses_require_exact_fields() {
    let server = MockServer::start().await;
    for (method_name, route, body) in [
        ("POST", "/tables", serde_json::json!({})),
        ("POST", "/tables/users/put", serde_json::json!({})),
        (
            "POST",
            "/tables/users/commit",
            serde_json::json!({"epoch": 7, "epoch_text": "8"}),
        ),
        (
            "POST",
            "/kit/create_table",
            serde_json::json!({"table_id": 7, "table_id_text": "07"}),
        ),
    ] {
        Mock::given(method(method_name))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
    }
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        for error in [
            client.create_table("users", Vec::new()).unwrap_err(),
            client.put("users", Vec::new()).unwrap_err(),
            client.commit("users").unwrap_err(),
            client.kit_create_table(&serde_json::json!({})).unwrap_err(),
        ] {
            assert!(matches!(
                error,
                ClientError::Kit {
                    code: KitErrorCode::QueryOutcomeUnknown,
                    committed: None,
                    ..
                }
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocking_legacy_write_responses_accept_exact_aliases() {
    let server = MockServer::start().await;
    for (route, body) in [
        (
            "/tables",
            serde_json::json!({"table_id": 9007199254740993_u64, "table_id_text": "9007199254740993"}),
        ),
        (
            "/tables/users/put",
            serde_json::json!({"row_id": "9007199254740993"}),
        ),
        (
            "/tables/users/commit",
            serde_json::json!({"epoch": 9007199254740993_u64, "epoch_text": "9007199254740993"}),
        ),
        (
            "/kit/create_table",
            serde_json::json!({"table_id": 9007199254740993_u64, "table_id_text": "9007199254740993"}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
    }
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        assert_eq!(
            client.create_table("users", Vec::new()).unwrap(),
            9_007_199_254_740_993
        );
        assert_eq!(
            client.put("users", Vec::new()).unwrap(),
            9_007_199_254_740_993
        );
        assert_eq!(client.commit("users").unwrap(), 9_007_199_254_740_993);
        assert_eq!(
            client.kit_create_table(&serde_json::json!({})).unwrap(),
            9_007_199_254_740_993
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocking_legacy_write_responses_require_text_aliases() {
    for (route, body, operation) in [
        (
            "/tables",
            serde_json::json!({"table_id": 7}),
            "create_table",
        ),
        (
            "/tables/users/commit",
            serde_json::json!({"epoch": 7}),
            "commit",
        ),
        (
            "/kit/create_table",
            serde_json::json!({"table_id": 7}),
            "kit_create_table",
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let operation = operation.to_owned();
        let error = tokio::task::spawn_blocking(move || {
            let client = MongrelClient::new(&uri).unwrap();
            match operation.as_str() {
                "create_table" => client.create_table("users", Vec::new()).unwrap_err(),
                "commit" => client.commit("users").unwrap_err(),
                "kit_create_table" => client.kit_create_table(&serde_json::json!({})).unwrap_err(),
                _ => unreachable!(),
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            error,
            ClientError::Kit {
                code: KitErrorCode::QueryOutcomeUnknown,
                committed: None,
                ..
            }
        ));
    }
}
