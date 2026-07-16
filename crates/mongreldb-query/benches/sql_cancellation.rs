use criterion::{criterion_group, criterion_main, Criterion};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use mongreldb_query::{CancelOutcome, MongrelSession, QueryId, SqlQueryOptions, SqlTestHookPoint};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn session() -> (tempfile::TempDir, Arc<MongrelSession>) {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::create(directory.path()).unwrap();
    database
        .create_table(
            "items",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    database
        .transaction(|transaction| {
            for chunk in (0..100_000_i64).collect::<Vec<_>>().chunks(1_000) {
                transaction.put_batch(
                    "items",
                    chunk
                        .iter()
                        .map(|id| vec![(1, Value::Int64(*id))])
                        .collect(),
                )?;
            }
            Ok(())
        })
        .unwrap();
    (
        directory,
        Arc::new(MongrelSession::open(Arc::new(database)).unwrap()),
    )
}

fn benchmarks(criterion: &mut Criterion) {
    let (_directory, session) = session();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = criterion.benchmark_group("sql_execution_control");
    group.bench_function("point_query", |benchmark| {
        benchmark.to_async(&runtime).iter(|| async {
            session.run("SELECT 1").await.unwrap();
        });
    });
    group.bench_function("scan_100k", |benchmark| {
        benchmark.to_async(&runtime).iter(|| async {
            session.clear_cache();
            session.run("SELECT sum(id * id) FROM items").await.unwrap();
        });
    });
    group.bench_function("cancel_scan_latency", |benchmark| {
        benchmark.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let barrier = Arc::new(Barrier::new(2));
                let hook_barrier = Arc::clone(&barrier);
                let fired = Arc::new(AtomicBool::new(false));
                let hook_fired = Arc::clone(&fired);
                let (entered_tx, entered_rx) = std::sync::mpsc::channel();
                session.set_test_hook(Some(Arc::new(move |point| {
                    if point == SqlTestHookPoint::BeforeScanBatch
                        && !hook_fired.swap(true, Ordering::AcqRel)
                    {
                        entered_tx.send(()).unwrap();
                        hook_barrier.wait();
                    }
                })));
                let query_id = QueryId::random().unwrap();
                session.clear_cache();
                let worker_session = Arc::clone(&session);
                let worker = runtime.spawn(async move {
                    worker_session
                        .run_with_options(
                            "SELECT sum(id * id) FROM items",
                            SqlQueryOptions {
                                query_id: Some(query_id),
                                timeout: Some(Duration::from_secs(10)),
                                ..SqlQueryOptions::default()
                            },
                        )
                        .await
                });
                entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                let started = Instant::now();
                assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
                barrier.wait();
                assert!(runtime.block_on(worker).unwrap().is_err());
                elapsed += started.elapsed();
                session.set_test_hook(None);
            }
            elapsed
        });
    });
    group.bench_function("cancel_queued_latency", |benchmark| {
        benchmark.iter_custom(|iterations| {
            let held = runtime
                .block_on(session.run_stream("SELECT id FROM items"))
                .unwrap();
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let query_id = QueryId::random().unwrap();
                let query = session
                    .register_query(SqlQueryOptions {
                        query_id: Some(query_id),
                        timeout: Some(Duration::from_secs(10)),
                        ..SqlQueryOptions::default()
                    })
                    .unwrap();
                let worker_session = Arc::clone(&session);
                let worker = runtime
                    .spawn(async move { worker_session.run_with_query("SELECT 1", query).await });
                while session
                    .query_registry()
                    .status(query_id)
                    .is_none_or(|status| status.phase != mongreldb_query::SqlQueryPhase::Queued)
                {
                    std::thread::yield_now();
                }
                let started = Instant::now();
                assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
                assert!(runtime.block_on(worker).unwrap().is_err());
                elapsed += started.elapsed();
            }
            drop(held);
            elapsed
        });
    });
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
