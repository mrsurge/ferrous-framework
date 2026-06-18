use ferrous_framework::{FerrousNativeManager, FerrousNativeProcConfig, FerrousNativeShellStatus};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn test_log_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let dir = std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("native-proc-tests")
        .join(format!("{name}-{unique}"));
    fs::create_dir_all(&dir).expect("test log dir");
    dir
}

fn base_config(command: Vec<String>, log_dir: PathBuf) -> FerrousNativeProcConfig {
    FerrousNativeProcConfig {
        command,
        cwd: None,
        env: HashMap::new(),
        label: "native-proc-test".to_owned(),
        spec_id: "native-proc-test".to_owned(),
        subgroups: vec!["tests".to_owned()],
        log_dir,
    }
}

#[test]
fn spawns_proc_and_captures_stdout_stderr_logs() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("captures-logs");
    let record = manager
        .spawn_proc_blocking(base_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "printf 'hello stdout'; printf 'hello stderr' >&2".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn proc");

    assert_eq!(record.backend, "proc");
    assert_eq!(record.status, FerrousNativeShellStatus::Running);
    assert!(!record.capabilities.stdin_write);
    assert!(record.capabilities.stdout_log);
    assert!(record.capabilities.stderr_log);

    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    assert_eq!(exited.exit_code, Some(0));

    let stdout = fs::read_to_string(&exited.stdout_log).expect("stdout log");
    let stderr = fs::read_to_string(&exited.stderr_log).expect("stderr log");
    assert_eq!(stdout, "hello stdout");
    assert_eq!(stderr, "hello stderr");

    let listed = manager.list_shells().expect("list shells");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, record.id);
}

#[test]
fn terminates_running_proc() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("terminates-proc");
    let record = manager
        .spawn_proc_blocking(base_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            log_dir,
        ))
        .expect("spawn proc");

    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    assert_ne!(exited.exit_code, Some(0));
}

#[test]
fn rejects_empty_proc_command() {
    let manager = FerrousNativeManager::new();
    let error = manager
        .spawn_proc_blocking(base_config(Vec::new(), test_log_dir("empty-command")))
        .expect_err("empty command should fail");
    assert!(error.to_string().contains("cannot be empty"));
}
