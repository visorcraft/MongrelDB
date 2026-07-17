//! Integration tests for the FND-006 fault-injection framework: barrier
//! coordination across threads, count-limited firing, scoped-guard panic
//! safety, hit counting, and the callback action.

use mongreldb_fault::{
    activate, activate_limited, clear, deactivate, hits, inject, wait_barrier, Action,
    BarrierAction, BarrierError, Fault, ScopedGuard,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The registry is process-global; serialize tests so armed hooks cannot
/// leak between them.
static TEST_LOCK: Mutex<()> = Mutex::new(());

const TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn barrier_coordinates_hook_hits_across_threads() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    activate(
        "test.barrier",
        Action::Barrier(BarrierAction::new("test.barrier")),
    );
    let worker = std::thread::spawn(|| {
        inject("test.barrier").unwrap();
        inject("test.barrier").unwrap();
    });
    // No sleeps: the main thread blocks until both hook hits arrive.
    wait_barrier("test.barrier", 2, TIMEOUT).unwrap();
    worker.join().unwrap();
    assert_eq!(hits("test.barrier"), 2);
    clear();
}

#[test]
fn barrier_waits_for_each_wave_of_arrivals() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    activate(
        "test.barrier.waves",
        Action::Barrier(BarrierAction::new("test.barrier.waves")),
    );
    let worker = std::thread::spawn(|| {
        for _ in 0..3 {
            inject("test.barrier.waves").unwrap();
        }
    });
    wait_barrier("test.barrier.waves", 1, TIMEOUT).unwrap();
    wait_barrier("test.barrier.waves", 3, TIMEOUT).unwrap();
    worker.join().unwrap();
    assert_eq!(hits("test.barrier.waves"), 3);
    clear();
}

#[test]
fn wait_barrier_times_out_without_arrivals() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    let timeout = Duration::from_millis(50);
    let error = wait_barrier("test.barrier.timeout", 1, timeout).unwrap_err();
    assert_eq!(
        error,
        BarrierError::Timeout {
            name: "test.barrier.timeout",
            expected: 1,
            arrived: 0,
            timeout,
        }
    );
}

#[test]
fn count_limit_fires_then_passes_through() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    activate_limited("test.limited", Action::Fail, 2);
    assert_eq!(inject("test.limited"), Err(Fault::Injected("test.limited")));
    assert_eq!(inject("test.limited"), Err(Fault::Injected("test.limited")));
    assert!(
        inject("test.limited").is_ok(),
        "budget exhausted: pass through"
    );
    assert!(inject("test.limited").is_ok());
    // Every evaluation counts as a hit, including pass-throughs.
    assert_eq!(hits("test.limited"), 4);
    clear();
}

#[test]
fn scoped_guard_clears_on_drop_and_survives_panic() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    {
        let guard = ScopedGuard::new("test.guard", Action::Fail);
        assert_eq!(guard.name(), "test.guard");
        assert_eq!(inject("test.guard"), Err(Fault::Injected("test.guard")));
    }
    assert!(inject("test.guard").is_ok(), "drop disarmed the hook");

    // Panic safety: the guard disarms while unwinding from an injected panic.
    let result = std::panic::catch_unwind(|| {
        let _guard = ScopedGuard::new("test.guard.panic", Action::Panic);
        let _ = inject("test.guard.panic");
    });
    assert!(result.is_err(), "the Panic action unwound");
    assert!(
        inject("test.guard.panic").is_ok(),
        "the guard cleared the registry during unwinding"
    );
    // The whole registry was cleared, including the first hook's state.
    assert_eq!(hits("test.guard"), 0);
}

#[test]
fn scoped_guard_limited_composes_count_limits() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    {
        let _guard = ScopedGuard::limited("test.guard.limited", Action::Fail, 1);
        assert!(inject("test.guard.limited").is_err());
        assert!(inject("test.guard.limited").is_ok());
    }
    assert!(inject("test.guard.limited").is_ok());
}

#[test]
fn callback_action_observes_the_hook_name() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    activate(
        "test.callback",
        Action::Callback(Arc::new(move |name| {
            observed.lock().unwrap().push(name);
        })),
    );
    assert!(inject("test.callback").is_ok());
    assert!(inject("test.callback").is_ok());
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &["test.callback", "test.callback"]
    );
    assert_eq!(hits("test.callback"), 2);
    clear();
}

#[test]
fn hit_counts_reset_on_rearm_and_deactivate() {
    let _permit = TEST_LOCK.lock().unwrap();
    clear();
    activate("test.hits", Action::Fail);
    let _ = inject("test.hits");
    assert_eq!(hits("test.hits"), 1);
    activate("test.hits", Action::Fail);
    assert_eq!(hits("test.hits"), 0, "re-arming resets the count");
    let _ = inject("test.hits");
    deactivate("test.hits");
    assert_eq!(hits("test.hits"), 0, "deactivate forgets the count");
    // An unrelated armed hook keeps the registry armed; unconfigured hooks
    // still evaluate to Ok and record no hits.
    activate("test.hits.other", Action::Fail);
    assert!(inject("test.hits").is_ok());
    assert_eq!(hits("test.hits"), 0);
    clear();
}
