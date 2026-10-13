use ferrous_framework::persistent_shell::{
    AttachmentState, ShutdownIntent, WriterLease, parse_survival, select_shutdown,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn replay_client_is_accessible_without_a_separate_supervisor_dependency() {
    use ferrous_framework::persistent_shell::{ReplayCursor, attachment, client::Client};
    let cursor = ReplayCursor {
        stdout: 4,
        stderr: 2,
    };
    assert_eq!(
        serde_json::to_value(cursor).unwrap(),
        serde_json::json!({"stdout":4,"stderr":2})
    );
    // Constructing a future must not open a socket or require a runtime yet.
    drop(Client::connect_from(
        "/not-opened",
        b"secret",
        "instance",
        cursor,
    ));
    drop(attachment::attach(
        "/not-opened",
        b"secret",
        "fs_test",
        None,
    ));
}

#[derive(Deserialize)]
struct SurvivalCase {
    value: Value,
    expected: Option<bool>,
}
#[derive(Deserialize)]
struct ShutdownCase {
    name: String,
    parents: BTreeMap<u32, Option<u32>>,
    roots: BTreeSet<u32>,
    survival_roots: BTreeSet<u32>,
    intent: ShutdownIntent,
    terminate: BTreeSet<u32>,
    preserved: BTreeSet<u32>,
}
#[derive(Deserialize)]
struct Cases {
    survival: Vec<SurvivalCase>,
    shutdown: Vec<ShutdownCase>,
}

#[test]
fn shared_policy_fixtures() {
    let cases: Cases =
        serde_json::from_str(include_str!("../testdata/persistent_shell_cases.json")).unwrap();
    for case in cases.survival {
        assert_eq!(
            parse_survival(&case.value).ok(),
            case.expected,
            "{}",
            case.value
        );
    }
    for case in cases.shutdown {
        let selection = select_shutdown(
            &case.parents,
            &case.roots,
            &case.survival_roots,
            case.intent,
        );
        assert_eq!(selection.terminate, case.terminate, "{}", case.name);
        assert_eq!(selection.preserved, case.preserved, "{}", case.name);
    }
}

#[test]
fn writer_fencing_and_disconnect_not_eof() {
    let mut state = AttachmentState::new("instance-a".into()).unwrap();
    let old = state.acquire("manager-a".into(), false).unwrap();
    assert!(state.acquire("manager-b".into(), false).is_err());
    let current = state.acquire("manager-b".into(), true).unwrap();
    assert!(!state.can_write(&old));
    state.detach(&old);
    assert!(state.can_write(&current));
    assert!(!state.can_write(&WriterLease {
        instance: "wrong-instance".into(),
        ..current.clone()
    }));
    assert!(state.eof(&old).is_err());
    state.detach(&current);
    let resumed = state.acquire("manager-c".into(), false).unwrap();
    assert!(resumed.generation > current.generation);
    assert!(state.can_write(&resumed));
    state.eof(&resumed).unwrap();
    assert!(!state.can_write(&resumed));
    assert!(state.acquire("manager-d".into(), true).is_err());
}

#[test]
fn exit_revokes_writer() {
    let mut state = AttachmentState::new("instance-a".into()).unwrap();
    let writer = state.acquire("manager-a".into(), false).unwrap();
    state.child_exited();
    assert!(!state.can_write(&writer));
    assert!(state.acquire("manager-b".into(), true).is_err());
}
