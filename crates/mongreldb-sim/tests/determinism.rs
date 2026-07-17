//! Integration proofs for the FND-005 determinism contract: same seed
//! ⇒ identical event logs, fault injection works, partitions isolate
//! and heal, crash/restart preserves only fsynced data, skew applies,
//! and failed scenarios persist a seed file.

use mongreldb_sim::{
    Event, LinkConfig, Network, NodeId, Scenario, ScenarioError, Seed, SimRng, SkewSchedule,
    TaskState,
};
use std::collections::BTreeSet;
use std::fs;

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);

/// A two-node workload with drops, duplicates, latency jitter, and disk
/// writes — rich enough that any nondeterminism would show in the log.
fn two_node_workload(seed: u64) -> Scenario {
    let mut scenario = Scenario::new(Seed::new(seed));
    scenario.set_link_config(
        A,
        B,
        LinkConfig {
            min_latency: 5,
            max_latency: 80,
            drop_per_mille: 100,
            duplicate_per_mille: 100,
        },
    );
    scenario.add_node(A, "sender", || {
        let mut sent = 0u32;
        Box::new(move |ctx| {
            if sent >= 6 {
                return TaskState::Done;
            }
            sent += 1;
            let _ = ctx.send(B, format!("m{sent}").into_bytes());
            ctx.log(format!("sent m{sent}"));
            TaskState::SleepFor(50)
        })
    });
    scenario.add_node(B, "receiver", || {
        Box::new(move |ctx| {
            while let Some(message) = ctx.try_recv() {
                let text = String::from_utf8_lossy(&message.payload).into_owned();
                ctx.log(format!("recv {text}"));
                ctx.disk_append("inbox.log", text.as_bytes())
                    .expect("append");
                ctx.disk_fsync("inbox.log").expect("fsync");
            }
            if ctx.now() >= 2_000 {
                TaskState::Done
            } else {
                TaskState::SleepFor(20)
            }
        })
    });
    scenario
}

#[test]
fn same_seed_produces_identical_event_logs() {
    let mut first = two_node_workload(42);
    first.run(100_000).expect("first run completes");
    let mut second = two_node_workload(42);
    second.run(100_000).expect("second run completes");

    assert!(!first.events().is_empty());
    assert_eq!(
        serde_json::to_string(first.events()).unwrap(),
        serde_json::to_string(second.events()).unwrap()
    );
    assert_eq!(
        first.disk(B).unwrap().read("inbox.log"),
        second.disk(B).unwrap().read("inbox.log")
    );
}

#[test]
fn different_seeds_produce_different_event_logs() {
    let mut first = two_node_workload(42);
    first.run(100_000).unwrap();
    let mut second = two_node_workload(43);
    second.run(100_000).unwrap();
    assert_ne!(
        serde_json::to_string(first.events()).unwrap(),
        serde_json::to_string(second.events()).unwrap()
    );
}

#[test]
fn drop_duplicate_and_reorder_injection_works() {
    let mut net = Network::new(LinkConfig::default());
    net.set_link_config(
        A,
        B,
        LinkConfig {
            min_latency: 1,
            max_latency: 1_000,
            drop_per_mille: 200,
            duplicate_per_mille: 200,
        },
    );
    let mut rng = SimRng::from_seed(Seed::new(7));
    for i in 0..50u8 {
        net.send(A, B, vec![i], 0, &mut rng);
    }

    let stats = net.stats();
    assert_eq!(stats.sent, 50);
    assert!(stats.dropped > 0, "expected drops, got {stats:?}");
    assert!(stats.duplicated > 0, "expected duplicates, got {stats:?}");
    assert!(stats.reordered > 0, "expected reorders, got {stats:?}");

    let deliveries = net.deliver_due(10_000, &BTreeSet::new());
    assert_eq!(
        deliveries.len() as u64,
        50 - stats.dropped + stats.duplicated
    );
    assert_eq!(net.stats().delivered as usize, deliveries.len());

    let mut drained = 0;
    while net.try_recv(B).is_some() {
        drained += 1;
    }
    assert_eq!(drained, deliveries.len());
}

#[test]
fn partition_isolates_then_heals() {
    let mut scenario = Scenario::new(Seed::new(5));
    scenario.set_link_config(A, B, LinkConfig::new(10, 10));
    // Sender emits id k at t=(k-1)*50 for k in 1..=20.
    scenario.add_node(A, "sender", || {
        let mut next = 0u8;
        Box::new(move |ctx| {
            if next >= 20 {
                return TaskState::Done;
            }
            next += 1;
            let _ = ctx.send(B, vec![next]);
            TaskState::SleepFor(50)
        })
    });
    scenario.add_node(B, "receiver", || {
        Box::new(move |ctx| {
            while let Some(message) = ctx.try_recv() {
                ctx.log(format!("recv {}", message.payload[0]));
            }
            if ctx.now() >= 1_500 {
                TaskState::Done
            } else {
                TaskState::SleepFor(25)
            }
        })
    });

    // Partition covers sends at t=400..=750 (ids 9..=16); heal lands
    // before the t=800 send (id 17).
    let mut partitioned = false;
    let mut healed = false;
    scenario
        .run_with(100_000, |s| {
            if !partitioned && s.now() >= 400 {
                s.partition([A], [B]);
                partitioned = true;
            }
            if !healed && s.now() >= 800 {
                s.heal();
                healed = true;
            }
        })
        .expect("partitioned run completes");
    assert!(partitioned && healed);

    let received: Vec<u8> = scenario
        .events()
        .iter()
        .filter_map(|event| match event {
            Event::Custom { message, .. } => {
                message.strip_prefix("recv ").map(|id| id.parse().unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(received, vec![1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20]);

    let stats = scenario.network_stats();
    assert_eq!(stats.sent, 20);
    assert_eq!(stats.dropped, 8);
    assert_eq!(stats.delivered, 12);
}

#[test]
fn crash_restart_preserves_only_fsynced_data() {
    let mut scenario = Scenario::new(Seed::new(11));
    scenario.add_node(A, "wal-writer", || {
        Box::new(move |ctx| {
            // On (re)start, recovery sees exactly the fsynced prefix.
            if ctx.disk_read_durable("wal") == b"ab" {
                ctx.log("recovered:ab");
                return TaskState::Done;
            }
            ctx.disk_append("wal", b"ab").expect("append durable part");
            ctx.disk_fsync("wal").expect("fsync durable part");
            ctx.disk_append("wal", b"cd").expect("append pending part");
            ctx.log("wrote-pending");
            TaskState::SleepFor(1_000_000)
        })
    });

    let mut bounced = false;
    scenario
        .run_with(10_000, |s| {
            if !bounced
                && s.events().iter().any(
                    |e| matches!(e, Event::Custom { message, .. } if message == "wrote-pending"),
                )
            {
                s.crash_node(A);
                s.restart_node(A);
                bounced = true;
            }
        })
        .expect("crash/restart run completes");
    assert!(bounced);

    let disk = scenario.disk(A).expect("node disk");
    assert_eq!(disk.read_durable("wal"), b"ab");
    assert_eq!(disk.read("wal"), b"ab");
    assert!(scenario
        .events()
        .iter()
        .any(|e| matches!(e, Event::Custom { message, .. } if message == "recovered:ab")));
    assert!(scenario
        .events()
        .iter()
        .any(|e| matches!(e, Event::Crash { node } if *node == A)));
    assert!(scenario
        .events()
        .iter()
        .any(|e| matches!(e, Event::Restart { node } if *node == A)));
}

#[test]
fn per_node_clock_skew_applies() {
    let mut scenario = Scenario::new(Seed::new(3));
    scenario.set_skew(B, SkewSchedule::constant(250));
    scenario.add_node(B, "clock-reader", || {
        let mut steps = 0;
        Box::new(move |ctx| {
            steps += 1;
            ctx.log(format!("raw={} local={}", ctx.now(), ctx.node_now()));
            if steps >= 2 {
                TaskState::Done
            } else {
                TaskState::SleepFor(100)
            }
        })
    });
    scenario.run(10_000).expect("run completes");

    let logs: Vec<&str> = scenario
        .events()
        .iter()
        .filter_map(|event| match event {
            Event::Custom { message, .. } => Some(message.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(logs, ["raw=0 local=250", "raw=100 local=350"]);
}

#[test]
fn failed_scenario_writes_a_seed_file() {
    let dir = std::env::temp_dir().join(format!(
        "mongreldb-sim-failures-test-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    std::env::set_var("MONGRELDB_SIM_FAILURES", &dir);

    let mut scenario = Scenario::new(Seed::new(999));
    scenario.add_node(A, "waiter", || Box::new(|_| TaskState::WaitForMessage));
    let error = scenario.run(1_000).unwrap_err();
    assert!(matches!(error, ScenarioError::DeadlockDetected { .. }));

    let entries: Vec<_> = fs::read_dir(&dir)
        .expect("failure dir exists")
        .map(Result::unwrap)
        .collect();
    assert_eq!(entries.len(), 1, "exactly one failure artifact");
    let file_name = entries[0].file_name().to_string_lossy().into_owned();
    assert!(
        file_name.contains("999"),
        "artifact name carries the seed: {file_name}"
    );

    let written: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(entries[0].path()).unwrap()).unwrap();
    assert_eq!(written["seed"], 999);
    assert!(written["error"].as_str().unwrap().contains("deadlock"));
    assert!(
        !written["events"].as_array().unwrap().is_empty(),
        "spawn events recorded"
    );

    let _ = fs::remove_dir_all(&dir);
}
