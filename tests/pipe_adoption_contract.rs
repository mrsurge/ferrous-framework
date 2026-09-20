//! Characterize ordinary pipe ownership before adding resumable transports.
use ferrous_framework::{
    FerrousNativeEnv, FerrousNativeManager, FerrousNativePipeConfig, FerrousNativeShellStatus,
    FerrousNativeStore,
};
use std::{collections::HashMap, time::Duration};

struct Cleanup(FerrousNativeManager, String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = self.0.terminate_shell_blocking(&self.1);
    }
}

#[test]
fn fresh_manager_observes_but_cannot_use_live_pipe() {
    let directory = tempfile::tempdir().unwrap();
    let secret = "disposable-adoption-contract".to_owned();
    let store = FerrousNativeStore::from_base_dir_fingerprint_secret(
        directory.path().to_owned(),
        "adoption-test".into(),
        secret.clone(),
    )
    .unwrap();
    let env = FerrousNativeEnv {
        secret,
        run_id: "adoption-test".into(),
        fws_socketio_url: None,
        te_framework_url: None,
        extra: HashMap::new(),
    };
    let owner = FerrousNativeManager::with_store_and_env(store.clone(), env.clone());
    let record = owner
        .spawn_pipe_blocking(FerrousNativePipeConfig {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while IFS= read -r line; do printf 'ack:%s\\n' \"$line\"; done".into(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "adoption-contract".into(),
            spec_id: "adoption-contract".into(),
            subgroups: vec!["isolated-test".into()],
            log_dir: None,
        })
        .unwrap();
    let cleanup = Cleanup(owner, record.id.clone());
    let owner = &cleanup.0;
    let fresh = FerrousNativeManager::with_store_and_env(store, env);
    let loaded = fresh.get_shell(&record.id).unwrap().unwrap();
    assert!(loaded.adopted);
    assert_eq!(
        (&loaded.id, loaded.pid, &loaded.label, &loaded.subgroups),
        (&record.id, record.pid, &record.label, &record.subgroups)
    );
    assert_eq!(loaded.status, FerrousNativeShellStatus::Running);
    assert!(fresh.get_pipe_state(&record.id).unwrap().is_none());
    assert!(!loaded.capabilities.stdin_write);
    assert!(!loaded.capabilities.stdin_eof);
    assert!(!loaded.capabilities.stdout_subscribe);
    assert!(!loaded.capabilities.output_read);
    assert!(
        fresh
            .write_to_pipe_blocking(&record.id, b"not-delivered\n")
            .is_err()
    );
    owner
        .write_to_pipe_blocking(&record.id, b"still-owned\n")
        .unwrap();
    assert_eq!(
        owner
            .read_line_blocking(&record.id, Duration::from_secs(5))
            .unwrap()
            .as_deref(),
        Some("ack:still-owned")
    );
    assert!(fresh.terminate_shell_blocking(&record.id).unwrap());
    let exited = owner
        .wait_shell_blocking(&record.id, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
}
