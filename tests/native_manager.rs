use ferrous_framework::{
    FerrousNativeEnv, FerrousNativeManager, FerrousNativePipeConfig, FerrousNativeProcConfig,
    FerrousNativePtyConfig, FerrousNativeShellStatus, FerrousNativeStore,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn test_log_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let dir = std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("native-manager-tests")
        .join(format!("{name}-{unique}"));
    fs::create_dir_all(&dir).expect("test log dir");
    dir
}

fn base_proc_config(command: Vec<String>, log_dir: PathBuf) -> FerrousNativeProcConfig {
    FerrousNativeProcConfig {
        command,
        cwd: None,
        env: HashMap::new(),
        label: "native-proc-test".to_owned(),
        spec_id: "native-proc-test".to_owned(),
        subgroups: vec!["tests".to_owned()],
        log_dir: Some(log_dir),
    }
}

fn base_pipe_config(command: Vec<String>, log_dir: PathBuf) -> FerrousNativePipeConfig {
    FerrousNativePipeConfig {
        command,
        cwd: None,
        env: HashMap::new(),
        label: "native-pipe-test".to_owned(),
        spec_id: "native-pipe-test".to_owned(),
        subgroups: vec!["tests".to_owned()],
        log_dir: Some(log_dir),
    }
}

fn base_pty_config(command: Vec<String>, log_dir: PathBuf) -> FerrousNativePtyConfig {
    FerrousNativePtyConfig {
        command,
        cwd: None,
        env: HashMap::new(),
        label: "native-pty-test".to_owned(),
        spec_id: "native-pty-test".to_owned(),
        subgroups: vec!["tests".to_owned()],
        log_dir: Some(log_dir),
    }
}

fn jsonrpc_echo_command() -> Vec<String> {
    vec![env!("CARGO_BIN_EXE_ferrous-jsonrpc-echo").to_owned()]
}

fn test_native_env() -> FerrousNativeEnv {
    FerrousNativeEnv {
        secret: "secret-from-manager".to_owned(),
        run_id: "run-from-manager".to_owned(),
        fws_socketio_url: Some("http://127.0.0.1:19099/fws_ws".to_owned()),
        te_framework_url: Some("http://127.0.0.1:19099".to_owned()),
        extra: HashMap::from([("FERROUS_EXTRA".to_owned(), "extra-from-manager".to_owned())]),
    }
}

fn test_manager_with_store(name: &str) -> FerrousNativeManager {
    let base_dir = test_log_dir(name).join("fws-base");
    let secret = format!("secret-for-{name}");
    let store = FerrousNativeStore::from_base_dir_fingerprint_secret(
        base_dir,
        "test_fingerprint".to_owned(),
        secret.clone(),
    )
    .expect("test native store");
    let env = FerrousNativeEnv {
        secret,
        run_id: format!("run-for-{name}"),
        fws_socketio_url: None,
        te_framework_url: None,
        extra: HashMap::new(),
    };
    FerrousNativeManager::with_store_and_env(store, env)
}

fn test_manager_with_native_env(name: &str, env: FerrousNativeEnv) -> FerrousNativeManager {
    let store = FerrousNativeStore::from_base_dir_fingerprint_secret(
        test_log_dir(name).join("fws-base"),
        "test_fingerprint".to_owned(),
        env.secret.clone(),
    )
    .expect("test native store");
    FerrousNativeManager::with_store_and_env(store, env)
}

#[test]
fn spawns_proc_and_captures_stdout_stderr_logs() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("captures-logs");
    let record = manager
        .spawn_proc_blocking(base_proc_config(
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
fn native_env_overlay_reaches_proc_children() {
    let manager = test_manager_with_native_env("proc-native-env-store", test_native_env());
    let log_dir = test_log_dir("proc-native-env");
    let mut config = base_proc_config(
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "printf '%s|%s|%s|%s|%s' \"$FRAMEWORK_SHELLS_SECRET\" \"$FRAMEWORK_SHELLS_RUN_ID\" \"$FRAMEWORK_SHELLS_FWS_SOCKETIO_URL\" \"$TE_FRAMEWORK_URL\" \"$FERROUS_EXTRA\"".to_owned(),
        ],
        log_dir,
    );
    config.env.insert(
        "FRAMEWORK_SHELLS_SECRET".to_owned(),
        "secret-from-shell".to_owned(),
    );
    let record = manager.spawn_proc_blocking(config).expect("spawn proc");
    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");

    assert_eq!(
        exited
            .env
            .get("FRAMEWORK_SHELLS_SECRET")
            .map(String::as_str),
        Some("secret-from-shell")
    );
    assert_eq!(
        eventually_read_to_string(&exited.stdout_log, Duration::from_secs(3)),
        "secret-from-shell|run-from-manager|http://127.0.0.1:19099/fws_ws|http://127.0.0.1:19099|extra-from-manager"
    );
}

#[test]
fn native_env_overlay_reaches_pipe_children() {
    let manager = test_manager_with_native_env("pipe-native-env-store", test_native_env());
    let log_dir = test_log_dir("pipe-native-env");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "printf '%s|%s|%s|%s\n' \"$FRAMEWORK_SHELLS_SECRET\" \"$FRAMEWORK_SHELLS_RUN_ID\" \"$FRAMEWORK_SHELLS_FWS_SOCKETIO_URL\" \"$TE_FRAMEWORK_URL\"; sleep 30".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pipe");

    let line = manager
        .read_line_blocking(&record.id, Duration::from_secs(3))
        .expect("read line")
        .expect("env line");
    assert_eq!(
        line,
        "secret-from-manager|run-from-manager|http://127.0.0.1:19099/fws_ws|http://127.0.0.1:19099"
    );
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn native_record_sidecar_is_persisted_without_env_values() {
    let manager = test_manager_with_native_env("record-sidecar-store", test_native_env());
    let log_dir = test_log_dir("record-sidecar");
    let mut config = base_proc_config(
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "printf persisted".to_owned(),
        ],
        log_dir.clone(),
    );
    config
        .env
        .insert("VISIBLE_KEY".to_owned(), "sensitive-value".to_owned());
    let record = manager.spawn_proc_blocking(config).expect("spawn proc");
    let initial: Value = serde_json::from_str(&eventually_read_to_string(
        &record.record_path,
        Duration::from_secs(3),
    ))
    .expect("initial record json");
    assert_eq!(
        initial.get("status").and_then(Value::as_str),
        Some("running")
    );
    assert_eq!(
        initial.get("run_id").and_then(Value::as_str),
        Some("run-from-manager")
    );
    let env_keys = initial
        .get("env_keys")
        .and_then(Value::as_array)
        .expect("env keys");
    assert!(
        env_keys
            .iter()
            .any(|key| key.as_str() == Some("VISIBLE_KEY"))
    );
    assert!(!initial.to_string().contains("sensitive-value"));
    assert!(!initial.to_string().contains("secret-from-manager"));

    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    let persisted: Value = serde_json::from_str(&eventually_read_to_string(
        &record.record_path,
        Duration::from_secs(3),
    ))
    .expect("exited record json");
    assert_eq!(
        persisted.get("status").and_then(Value::as_str),
        Some("exited")
    );
    assert_eq!(persisted.get("exit_code").and_then(Value::as_i64), Some(0));
}

#[test]
fn omitted_log_dir_uses_fws_runtime_store_logs_dir() {
    let manager = test_manager_with_store("default-log-dir");
    let store = manager.store();
    assert_eq!(store.repo_fingerprint, "test_fingerprint");
    assert!(
        store
            .secret_file
            .ends_with("runtimes/test_fingerprint/secret")
    );
    assert_eq!(
        fs::read_to_string(&store.secret_file).expect("stored secret"),
        "secret-for-default-log-dir"
    );
    assert_eq!(store.logs_dir, store.root.join("logs"));
    let mut config = base_proc_config(
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "printf default-log-dir".to_owned(),
        ],
        test_log_dir("unused-explicit-log-dir"),
    );
    config.log_dir = None;
    let record = manager.spawn_proc_blocking(config).expect("spawn proc");

    assert!(record.record_path.starts_with(&store.logs_dir));
    assert!(record.stdout_log.starts_with(&store.logs_dir));
    assert!(record.stderr_log.starts_with(&store.logs_dir));

    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
}

#[test]
fn fresh_manager_lists_persisted_records_as_adopted_stale_records() {
    let manager = test_manager_with_store("persisted-record-list");
    let mut config = base_pipe_config(
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "while IFS= read -r line; do printf 'ack:%s\n' \"$line\"; done".to_owned(),
        ],
        test_log_dir("unused-persisted-record-list"),
    );
    config.log_dir = None;
    let record = manager.spawn_pipe_blocking(config).expect("spawn pipe");
    assert!(
        manager
            .write_line_blocking(&record.id, "first")
            .expect("write line")
    );
    let line = manager
        .read_line_blocking(&record.id, Duration::from_secs(3))
        .expect("read line")
        .expect("line");
    assert_eq!(line, "ack:first");
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

    let fresh = FerrousNativeManager::with_store_and_env(manager.store(), manager.native_env());
    let loaded = fresh
        .get_shell(&record.id)
        .expect("get persisted shell")
        .expect("persisted shell");
    assert!(loaded.adopted);
    assert_eq!(loaded.id, record.id);
    assert_eq!(loaded.backend, "pipe");
    assert_eq!(loaded.status, FerrousNativeShellStatus::Exited);
    assert!(!loaded.capabilities.stdin_write);
    assert!(!loaded.capabilities.terminate);
    assert!(loaded.capabilities.stdout_log);
    assert!(loaded.env.is_empty());
    assert!(
        loaded
            .env_keys
            .iter()
            .any(|key| key == "FRAMEWORK_SHELLS_RUN_ID")
    );
    assert_eq!(
        fresh
            .write_line_blocking(&record.id, "second")
            .expect("stale write should report unavailable"),
        false
    );
    assert_eq!(
        fresh
            .terminate_shell_blocking(&record.id)
            .expect("stale terminate should report unavailable"),
        false
    );

    let listed = fresh.list_shells().expect("list persisted shells");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, record.id);
    assert!(listed[0].adopted);
}

#[test]
fn terminates_running_proc() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("terminates-proc");
    let record = manager
        .spawn_proc_blocking(base_proc_config(
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
        .spawn_proc_blocking(base_proc_config(Vec::new(), test_log_dir("empty-command")))
        .expect_err("empty command should fail");
    assert!(error.to_string().contains("cannot be empty"));
}

#[test]
fn pipe_writes_stdin_and_reads_stdout_lines() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-echo");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "while IFS= read -r line; do printf 'ack:%s\n' \"$line\"; done".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pipe");

    assert_eq!(record.backend, "pipe");
    assert!(record.capabilities.stdin_write);
    assert!(
        manager
            .write_line_blocking(&record.id, r#"{"jsonrpc":"2.0","id":1}"#)
            .expect("write line")
    );
    let line = manager
        .read_line_blocking(&record.id, Duration::from_secs(3))
        .expect("read line")
        .expect("line");
    assert_eq!(line, r#"ack:{"jsonrpc":"2.0","id":1}"#);

    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn pty_writes_stdin_and_reads_output_lines() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pty-echo");
    let record = manager
        .spawn_pty_blocking(base_pty_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "stty -echo; while IFS= read -r line; do printf 'pty:%s\n' \"$line\"; done"
                    .to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pty");

    assert_eq!(record.backend, "pty");
    assert!(record.capabilities.stdin_write);
    manager
        .write_line_blocking(&record.id, "hello-pty")
        .expect("write pty");
    let mut line = String::new();
    for _ in 0..4 {
        let next = manager
            .read_line_blocking(&record.id, Duration::from_secs(3))
            .expect("read pty line")
            .expect("pty line");
        if next.starts_with("pty:") {
            line = next;
            break;
        }
    }
    assert_eq!(line, "pty:hello-pty");

    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn read_line_times_out_without_output() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-timeout");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            log_dir,
        ))
        .expect("spawn pipe");
    let line = manager
        .read_line_blocking(&record.id, Duration::from_millis(50))
        .expect("read timeout");
    assert_eq!(line, None);
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn proc_rejects_line_io() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("proc-line-io");
    let record = manager
        .spawn_proc_blocking(base_proc_config(
            vec!["sh".to_owned(), "-c".to_owned(), "printf done".to_owned()],
            log_dir,
        ))
        .expect("spawn proc");
    let error = manager
        .write_line_blocking(&record.id, "ping")
        .expect_err("proc write must fail");
    assert!(error.to_string().contains("does not expose stdin"));
}

#[test]
fn pipe_handles_jsonrpc_request_response_and_notifications() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-jsonrpc");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(jsonrpc_echo_command(), log_dir))
        .expect("spawn pipe");

    let request_count = 24;
    let bench_started = Instant::now();
    let mut issued_at = HashMap::new();
    let mut latencies = Vec::new();
    for id in 1..=request_count {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "bench.echo",
            "params": {
                "echo": format!("request-{id}"),
                "push_count": 2,
                "payload_size": 256
            }
        });
        assert!(
            manager
                .write_line_blocking(&record.id, &request.to_string())
                .expect("write request")
        );
        issued_at.insert(id as i64, Instant::now());
    }

    let mut responses = HashMap::new();
    let mut notifications = 0;
    while responses.len() < request_count {
        let line = manager
            .read_line_blocking(&record.id, Duration::from_secs(3))
            .expect("read line")
            .expect("jsonrpc line");
        let message: Value = serde_json::from_str(&line).expect("jsonrpc json");
        if message.get("method").and_then(Value::as_str) == Some("bench.push") {
            notifications += 1;
            continue;
        }
        let id = message
            .get("id")
            .and_then(Value::as_i64)
            .expect("response id");
        let result = message.get("result").expect("result");
        assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            result.get("echo").and_then(Value::as_str),
            Some(format!("request-{id}").as_str())
        );
        assert_eq!(
            result
                .get("payload")
                .and_then(Value::as_str)
                .expect("payload")
                .len(),
            256
        );
        let issued = issued_at.remove(&id).expect("issued timestamp");
        latencies.push(issued.elapsed());
        responses.insert(id, true);
    }

    let total_elapsed = bench_started.elapsed();
    let stats = duration_stats(&latencies);
    eprintln!(
        "ferrous_native_pipe_jsonrpc requests={} notifications={} elapsed_ms={:.3} throughput_rps={:.1} min_ms={:.3} p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        request_count,
        notifications,
        total_elapsed.as_secs_f64() * 1000.0,
        request_count as f64 / total_elapsed.as_secs_f64(),
        stats.min_ms,
        stats.p50_ms,
        stats.p95_ms,
        stats.max_ms,
    );
    assert_eq!(notifications, request_count * 2);
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn pipe_reports_sequential_jsonrpc_rtt_metrics() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-jsonrpc-rtt");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(jsonrpc_echo_command(), log_dir))
        .expect("spawn pipe");

    let request_count = 24;
    let mut latencies = Vec::new();
    let bench_started = Instant::now();
    for id in 1..=request_count {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "bench.echo",
            "params": {
                "echo": format!("sequential-{id}"),
                "push_count": 0,
                "payload_size": 256
            }
        });
        let started = Instant::now();
        manager
            .write_line_blocking(&record.id, &request.to_string())
            .expect("write request");
        let line = manager
            .read_line_blocking(&record.id, Duration::from_secs(3))
            .expect("read line")
            .expect("response line");
        latencies.push(started.elapsed());

        let response: Value = serde_json::from_str(&line).expect("response json");
        assert_eq!(response.get("id").and_then(Value::as_i64), Some(id as i64));
        assert_eq!(
            response
                .get("result")
                .and_then(|result| result.get("echo"))
                .and_then(Value::as_str),
            Some(format!("sequential-{id}").as_str())
        );
    }

    let total_elapsed = bench_started.elapsed();
    let stats = duration_stats(&latencies);
    eprintln!(
        "ferrous_native_pipe_jsonrpc_rtt requests={} elapsed_ms={:.3} throughput_rps={:.1} min_ms={:.3} p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        request_count,
        total_elapsed.as_secs_f64() * 1000.0,
        request_count as f64 / total_elapsed.as_secs_f64(),
        stats.min_ms,
        stats.p50_ms,
        stats.p95_ms,
        stats.max_ms,
    );

    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[derive(Clone, Copy, Debug, Default)]
struct DurationStats {
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

fn duration_stats(samples: &[Duration]) -> DurationStats {
    if samples.is_empty() {
        return DurationStats::default();
    }
    let mut millis = samples
        .iter()
        .map(|sample| sample.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    millis.sort_by(|left, right| left.total_cmp(right));
    DurationStats {
        min_ms: millis[0],
        p50_ms: percentile(&millis, 0.50),
        p95_ms: percentile(&millis, 0.95),
        max_ms: millis[millis.len() - 1],
    }
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.len() == 1 {
        return sorted[0];
    }
    let position = (sorted.len() - 1) as f64 * q;
    let lower = position.floor() as usize;
    let upper = usize::min(lower + 1, sorted.len() - 1);
    let weight = position - lower as f64;
    sorted[lower] * (1.0 - weight) + sorted[upper] * weight
}

fn eventually_read_to_string(path: &PathBuf, timeout: Duration) -> String {
    let started = Instant::now();
    loop {
        let content = fs::read_to_string(path).unwrap_or_default();
        if !content.is_empty() || started.elapsed() >= timeout {
            return content;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
