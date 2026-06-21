use ferrous_framework::{
    FerrousFrameworkPipe, FerrousNativeEnv, FerrousNativeLifecycleEventKind, FerrousNativeManager,
    FerrousNativeOutputStream, FerrousNativePipeConfig, FerrousNativeProcConfig,
    FerrousNativePtyConfig, FerrousNativePtyMode, FerrousNativeShellStatus, FerrousNativeStore,
    FerrousPipeConfig, FerrousShellLaunchOverrides, load_persisted_record,
    shellspec::ShellspecRenderInput,
};
use serde_json::{Value, json};
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
        mode: FerrousNativePtyMode::Interactive,
    }
}

fn jsonrpc_echo_command() -> Vec<String> {
    vec![env!("CARGO_BIN_EXE_ferrous-jsonrpc-echo").to_owned()]
}

fn tcp_ready_command() -> String {
    env!("CARGO_BIN_EXE_ferrous-tcp-ready").to_owned()
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
    let manager = test_manager_with_store("captures-logs-store");
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
    assert!(!record.capabilities.stdin_eof);
    assert!(record.capabilities.stdout_log);
    assert!(record.capabilities.stderr_log);
    assert!(record.capabilities.stdout_subscribe);
    assert!(record.capabilities.stderr_subscribe);
    assert!(!record.capabilities.output_read);
    assert!(record.capabilities.terminate);
    assert!(!record.capabilities.resize);

    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    assert_eq!(exited.exit_code, Some(0));

    let stdout = eventually_read_to_string(&exited.stdout_log, Duration::from_secs(3));
    let stderr = eventually_read_to_string(&exited.stderr_log, Duration::from_secs(3));
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
fn env_map_builder_uses_explicit_fws_store_and_child_env() {
    let base_dir = test_log_dir("env-map-builder-base");
    let env = HashMap::from([
        (
            "FRAMEWORK_SHELLS_BASE_DIR".to_owned(),
            base_dir.to_string_lossy().into_owned(),
        ),
        (
            "FRAMEWORK_SHELLS_REPO_FINGERPRINT".to_owned(),
            "env_map_fingerprint".to_owned(),
        ),
        (
            "FRAMEWORK_SHELLS_SECRET".to_owned(),
            "secret-from-env-map".to_owned(),
        ),
        (
            "FRAMEWORK_SHELLS_RUN_ID".to_owned(),
            "run-from-env-map".to_owned(),
        ),
    ]);
    let manager = FerrousNativeManager::try_with_env_map(&env).expect("manager from env map");
    assert_eq!(manager.store().repo_fingerprint, "env_map_fingerprint");
    assert_eq!(manager.native_env().secret, "secret-from-env-map");
    assert_eq!(manager.native_env().run_id, "run-from-env-map");

    let record = manager
        .spawn_proc_blocking(FerrousNativeProcConfig {
            command: vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "printf '%s|%s' \"$FRAMEWORK_SHELLS_SECRET\" \"$FRAMEWORK_SHELLS_RUN_ID\""
                    .to_owned(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "env-map-proc".to_owned(),
            spec_id: "env-map-proc".to_owned(),
            subgroups: vec!["tests".to_owned()],
            log_dir: None,
        })
        .expect("spawn proc");
    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(
        eventually_read_to_string(&exited.stdout_log, Duration::from_secs(3)),
        "secret-from-env-map|run-from-env-map"
    );
}

#[test]
fn async_manager_facade_matches_python_fws_pipe_shape() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async {
        let manager = test_manager_with_store("async-pipe-facade");
        let record = manager
            .spawn_shell_pipe(base_pipe_config(
                vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "while IFS= read -r line; do printf 'async:%s\\n' \"$line\"; done".to_owned(),
                ],
                test_log_dir("unused-async-pipe-facade"),
            ))
            .await
            .expect("spawn pipe");
        let state = manager
            .get_pipe_state(&record.id)
            .expect("pipe state")
            .expect("live pipe state");
        assert!(state.stdin_supported);
        assert_eq!(state.backend, "pipe");

        let result = manager
            .write_to_shell(&record.id, "first", true)
            .await
            .expect("write shell");
        assert!(result.accepted);
        assert_eq!(result.bytes_written, "first\n".len());
        assert!(result.newline_appended);
        assert!(!result.eof_sent);
        assert_eq!(
            manager
                .read_line_blocking(&record.id, Duration::from_secs(3))
                .expect("read line"),
            Some("async:first".to_owned())
        );

        manager
            .write_to_pipe(&record.id, "second\n")
            .await
            .expect("write pipe");
        assert_eq!(
            manager
                .read_line_blocking(&record.id, Duration::from_secs(3))
                .expect("read line"),
            Some("async:second".to_owned())
        );

        let error = manager
            .write_to_pipe("missing-shell", "nope\n")
            .await
            .expect_err("missing shell should be strict error");
        assert!(error.to_string().contains("not found"));
        manager
            .terminate_shell(&record.id, true)
            .await
            .expect("terminate shell");
    });
}

#[test]
fn native_record_meta_is_persisted_with_fws_shape() {
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
    assert_eq!(
        initial.get("runtime_id").and_then(Value::as_str),
        Some(manager.store().runtime_id.as_str())
    );
    assert_eq!(
        initial.get("autostart").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(initial.get("backend").and_then(Value::as_str), Some("proc"));
    assert_eq!(
        initial.get("uses_pty").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        initial.get("uses_pipes").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        initial.get("uses_dtach").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(initial.get("app_id").and_then(Value::as_str), Some("tests"));
    assert_eq!(
        initial.get("is_app_worker").and_then(Value::as_bool),
        Some(false)
    );
    assert!(initial.get("ui").and_then(Value::as_object).is_some());
    assert!(initial.get("debug").and_then(Value::as_object).is_some());
    assert_eq!(
        initial
            .get("env_overrides")
            .and_then(Value::as_object)
            .and_then(|env| env.get("VISIBLE_KEY"))
            .and_then(Value::as_str),
        Some("sensitive-value")
    );
    assert!(
        initial
            .get("io_metadata_log")
            .and_then(Value::as_str)
            .is_some_and(|path| path.ends_with(".io_metadata.jsonl"))
    );
    assert!(
        initial
            .get("created_at")
            .and_then(Value::as_f64)
            .is_some_and(|value| value > 0.0)
    );
    assert!(
        initial
            .get("updated_at")
            .and_then(Value::as_f64)
            .is_some_and(|value| value > 0.0)
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
    assert!(!initial.to_string().contains("secret-from-manager"));
    assert!(
        record
            .record_path
            .starts_with(&manager.store().metadata_dir)
    );

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

    assert!(record.record_path.starts_with(&store.metadata_dir));
    assert!(record.stdout_log.starts_with(&store.logs_dir));
    assert!(record.stderr_log.starts_with(&store.logs_dir));

    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
}

#[test]
fn shellspec_entry_launches_native_pipe_with_rendered_ctx_and_env() {
    let manager = test_manager_with_store("shellspec-pipe-launch");
    let document = json!({
        "version": "1",
        "shells": {
            "jsonrpc_pipe": {
                "backend": "pipe",
                "command": ["sh", "-c", "while IFS= read -r line; do printf '%s:%s\\n' \"$APP_ID\" \"$line\"; done"],
                "env": {
                    "APP_ID": "${ctx:APP_ID}",
                    "FROM_ENV": "${env:FROM_ENV}"
                },
                "subgroups": ["pipe", "${APP_ID}"]
            }
        }
    });
    let input = ShellspecRenderInput {
        ctx: HashMap::from([("APP_ID".to_owned(), "native-shellspec".to_owned())]),
        env: HashMap::from([("FROM_ENV".to_owned(), "external".to_owned())]),
    };
    let record = manager
        .spawn_shellspec_entry_blocking(&document, "jsonrpc_pipe", &input)
        .expect("spawn shellspec pipe");

    assert_eq!(record.backend, "pipe");
    assert_eq!(record.spec_id, "jsonrpc_pipe");
    assert_eq!(record.label, "jsonrpc_pipe");
    assert_eq!(record.subgroups, vec!["pipe", "native-shellspec"]);
    assert!(record.stdout_log.starts_with(manager.logs_dir()));
    assert_eq!(
        record.env.get("APP_ID").map(String::as_str),
        Some("native-shellspec")
    );
    assert_eq!(
        record.env.get("FROM_ENV").map(String::as_str),
        Some("external")
    );

    manager
        .write_line_blocking(&record.id, "ping")
        .expect("write line");
    let line = manager
        .read_line_blocking(&record.id, Duration::from_secs(3))
        .expect("read line")
        .expect("line");
    assert_eq!(line, "native-shellspec:ping");
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn shellspec_app_worker_launch_persists_python_fws_metadata_shape() {
    let manager = test_manager_with_store("shellspec-app-worker-metadata");
    let app_id = "file_editor_cm6";
    let document = json!({
        "version": "1",
        "shells": {
            "app-worker": {
                "backend": "proc",
                "command": ["sh", "-c", "printf app-worker-ready; sleep 30"],
                "env": {
                    "TE_APP_ID": "${ctx:APP_ID}",
                    "TE_APP_WORKER_PORT": "${free_port}"
                },
                "subgroups": ["${ctx:APP_ID}", "app-worker"],
                "ui": {
                    "subgroup_styles": {
                        "app-worker": { "bg": "blue" }
                    }
                },
                "debug": {
                    "io_metadata": true
                }
            }
        }
    });
    let input = ShellspecRenderInput {
        ctx: HashMap::from([("APP_ID".to_owned(), app_id.to_owned())]),
        env: HashMap::new(),
    };
    let record = manager
        .spawn_shellspec_entry_with_overrides_blocking(
            &document,
            "app-worker",
            &input,
            FerrousShellLaunchOverrides {
                env: HashMap::from([(
                    "TE_FRAMEWORK_URL".to_owned(),
                    "http://127.0.0.1:8089".to_owned(),
                )]),
                label: Some(format!("app-worker:{app_id}")),
                spec_id: Some(format!("app:{app_id}:app-worker")),
                subgroups: Some(vec![app_id.to_owned(), "app-worker".to_owned()]),
                ui: Some(
                    json!({
                        "card": "from-app-manifest"
                    })
                    .as_object()
                    .expect("ui object")
                    .clone(),
                ),
                debug: None,
                parent_shell_id: None,
            },
        )
        .expect("spawn app worker shellspec");

    assert_eq!(record.label, "app-worker:file_editor_cm6");
    assert_eq!(record.spec_id, "app:file_editor_cm6:app-worker");
    assert_eq!(record.subgroups, vec!["file_editor_cm6", "app-worker"]);
    assert_eq!(record.app_id.as_deref(), Some(app_id));
    assert!(record.is_app_worker);
    assert_eq!(
        record
            .env_overrides
            .get("TE_FRAMEWORK_URL")
            .map(String::as_str),
        Some("http://127.0.0.1:8089")
    );
    assert!(record.env_overrides.contains_key("TE_APP_WORKER_PORT"));
    assert!(
        record
            .record_path
            .starts_with(&manager.store().metadata_dir)
    );
    assert_eq!(
        record.record_path,
        manager
            .store()
            .metadata_dir
            .join(&record.id)
            .join("meta.json")
    );

    let persisted: Value = serde_json::from_str(&eventually_read_to_string(
        &record.record_path,
        Duration::from_secs(3),
    ))
    .expect("persisted app worker record");
    assert_eq!(
        persisted.get("label").and_then(Value::as_str),
        Some("app-worker:file_editor_cm6")
    );
    assert_eq!(
        persisted.get("spec_id").and_then(Value::as_str),
        Some("app:file_editor_cm6:app-worker")
    );
    assert_eq!(
        persisted.get("is_app_worker").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        persisted.get("app_id").and_then(Value::as_str),
        Some(app_id)
    );
    assert_eq!(
        persisted
            .get("subgroups")
            .and_then(Value::as_array)
            .and_then(|values| values.get(1))
            .and_then(Value::as_str),
        Some("app-worker")
    );
    assert!(
        persisted
            .get("env_overrides")
            .and_then(Value::as_object)
            .is_some_and(|env| env.contains_key("TE_APP_WORKER_PORT"))
    );
    assert_eq!(
        persisted
            .get("env_overrides")
            .and_then(Value::as_object)
            .and_then(|env| env.get("TE_FRAMEWORK_URL"))
            .and_then(Value::as_str),
        Some("http://127.0.0.1:8089")
    );
    assert!(
        persisted
            .get("ui")
            .and_then(Value::as_object)
            .is_some_and(|ui| ui.contains_key("subgroup_styles") && ui.contains_key("card"))
    );
    assert_eq!(
        persisted
            .get("debug")
            .and_then(Value::as_object)
            .and_then(|debug| debug.get("io_metadata"))
            .and_then(Value::as_bool),
        Some(true)
    );

    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn shellspec_entry_launches_command_string_proc_with_shlex_split() {
    let manager = test_manager_with_store("shellspec-proc-command-string");
    let document = json!({
        "version": "1",
        "shells": {
            "worker": {
                "backend": "proc",
                "command": "sh -c 'printf shellspec:$APP_ID'",
                "env": {
                    "APP_ID": "${APP_ID}"
                }
            }
        }
    });
    let input = ShellspecRenderInput {
        ctx: HashMap::from([("APP_ID".to_owned(), "proc-app".to_owned())]),
        env: HashMap::new(),
    };
    let record = manager
        .spawn_shellspec_entry_blocking(&document, "worker", &input)
        .expect("spawn shellspec proc");
    let exited = manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");

    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    assert_eq!(
        eventually_read_to_string(&exited.stdout_log, Duration::from_secs(3)),
        "shellspec:proc-app"
    );
}

#[test]
fn shellspec_entry_rejects_autostart_false() {
    let manager = test_manager_with_store("shellspec-autostart-false");
    let document = json!({
        "version": "1",
        "shells": {
            "disabled": {
                "backend": "proc",
                "command": ["sh", "-c", "printf no"],
                "autostart": false
            }
        }
    });
    let error = manager
        .spawn_shellspec_entry_blocking(&document, "disabled", &ShellspecRenderInput::default())
        .expect_err("autostart false should not launch");
    assert!(error.to_string().contains("autostart=false"));
}

#[test]
fn shellspec_entry_waits_for_stdout_regex_readiness() {
    let manager = test_manager_with_store("shellspec-stdout-readiness");
    let document = json!({
        "version": "1",
        "shells": {
            "worker": {
                "backend": "proc",
                "command": ["sh", "-c", "sleep 0.1; printf 'service READY'; sleep 0.2"],
                "readiness": {
                    "type": "stdout_regex",
                    "pattern": "READY",
                    "timeout": 2
                }
            }
        }
    });
    let record = manager
        .spawn_shellspec_entry_blocking(&document, "worker", &ShellspecRenderInput::default())
        .expect("spawn shellspec proc with stdout readiness");
    assert_eq!(record.backend, "proc");
    assert_eq!(
        eventually_read_to_string(&record.stdout_log, Duration::from_secs(3)),
        "service READY"
    );
}

#[test]
fn shellspec_entry_waits_for_tcp_port_readiness() {
    let manager = test_manager_with_store("shellspec-tcp-readiness");
    let helper = tcp_ready_command();
    let document = json!({
        "version": "1",
        "shells": {
            "tcp_worker": {
                "backend": "proc",
                "command": [helper, "${free_port}"],
                "env": {
                    "PORT": "${free_port}"
                },
                "readiness": {
                    "type": "tcp_port",
                    "host": "127.0.0.1",
                    "port": "${free_port}",
                    "timeout": 2
                }
            }
        }
    });
    let record = manager
        .spawn_shellspec_entry_blocking(&document, "tcp_worker", &ShellspecRenderInput::default())
        .expect("spawn shellspec proc with tcp readiness");
    assert_eq!(record.backend, "proc");
    assert_eq!(
        record.command.get(1).map(String::as_str),
        record.env.get("PORT").map(String::as_str)
    );
}

#[test]
fn shellspec_entry_launches_native_pty_with_rendered_mode() {
    let manager = test_manager_with_store("shellspec-pty-mode");
    let document = json!({
        "version": "1",
        "shells": {
            "terminal": {
                "backend": "pty",
                "pty_mode": "raw",
                "command": ["sh", "-c", "stty -a"]
            }
        }
    });
    let record = manager
        .spawn_shellspec_entry_blocking(&document, "terminal", &ShellspecRenderInput::default())
        .expect("spawn shellspec pty");
    assert_eq!(record.backend, "pty");
    assert_eq!(record.pty_mode, Some(FerrousNativePtyMode::Raw));
    let output = read_chunks_until_contains(&manager, &record.id, "icanon", Duration::from_secs(3))
        .expect("read stty output")
        .expect("stty output");
    assert!(output.contains("-icanon"), "stty output: {output}");
}

#[test]
fn shellspec_apply_starts_autostart_specs_once() {
    let manager = test_manager_with_store("shellspec-apply-starts-once");
    let document = json!({
        "version": "1",
        "shells": {
            "disabled": {
                "backend": "proc",
                "command": ["sh", "-c", "sleep 30"],
                "autostart": false
            },
            "worker": {
                "backend": "pipe",
                "command": ["sh", "-c", "while IFS= read -r line; do printf 'worker:%s\\n' \"$line\"; done"]
            }
        }
    });

    let started = manager
        .apply_shellspec_document_blocking(&document, &ShellspecRenderInput::default(), false)
        .expect("apply shellspec");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].spec_id, "worker");
    assert_eq!(started[0].backend, "pipe");

    let second = manager
        .apply_shellspec_document_blocking(&document, &ShellspecRenderInput::default(), false)
        .expect("second apply shellspec");
    assert!(second.is_empty());
    assert_eq!(manager.live_records().expect("live records").len(), 1);

    manager
        .write_line_blocking(&started[0].id, "ping")
        .expect("write line");
    let line = manager
        .read_line_blocking(&started[0].id, Duration::from_secs(3))
        .expect("read line")
        .expect("line");
    assert_eq!(line, "worker:ping");
    assert!(
        manager
            .terminate_shell_blocking(&started[0].id)
            .expect("terminate shell")
    );
}

#[test]
fn shellspec_apply_prunes_live_specs_not_in_desired_set() {
    let manager = test_manager_with_store("shellspec-apply-prune");
    let original = json!({
        "version": "1",
        "shells": {
            "worker": {
                "backend": "proc",
                "command": ["sh", "-c", "sleep 30"]
            }
        }
    });
    let replacement = json!({
        "version": "1",
        "shells": {
            "other": {
                "backend": "proc",
                "command": ["sh", "-c", "sleep 30"],
                "autostart": false
            }
        }
    });

    let started = manager
        .apply_shellspec_document_blocking(&original, &ShellspecRenderInput::default(), false)
        .expect("apply original");
    assert_eq!(started.len(), 1);
    let pruned_start = manager
        .apply_shellspec_document_blocking(&replacement, &ShellspecRenderInput::default(), true)
        .expect("apply replacement with prune");
    assert!(pruned_start.is_empty());
    let exited = manager
        .wait_shell_blocking(&started[0].id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
}

#[test]
fn persisted_python_fws_record_with_trailing_junk_is_readable() {
    let root = test_log_dir("python-fws-record-read");
    let metadata_dir = root.join("meta").join("fs_python_record");
    let logs_dir = root.join("logs");
    fs::create_dir_all(&metadata_dir).expect("create metadata dir");
    fs::create_dir_all(&logs_dir).expect("create logs dir");
    let record_path = metadata_dir.join("meta.json");
    let stdout_log = logs_dir.join("fs_python_record.stdout.log");
    let stderr_log = logs_dir.join("fs_python_record.stderr.log");
    fs::write(&stdout_log, "").expect("stdout log");
    fs::write(&stderr_log, "").expect("stderr log");
    fs::write(
        &record_path,
        format!(
            r#"{{
  "id": "fs_python_record",
  "spec_id": "app:file_editor_cm6:app-worker",
  "command": ["python", "-m", "app.libs.app_worker"],
  "label": "app-worker:file_editor_cm6",
  "subgroups": ["file_editor_cm6", "app-worker"],
  "ui": {{"subgroup_styles": {{"lsp": {{"bg": "blue"}}}}}},
  "cwd": "/tmp/project",
  "env_overrides": {{
    "TE_APP_ID": "file_editor_cm6",
    "TE_APP_WORKER_PORT": "42401"
  }},
  "pid": 99999999,
  "status": "running",
  "created_at": 1778486289.4786,
  "updated_at": 1778486289.5455,
  "autostart": true,
  "stdout_log": "{}",
  "stderr_log": "{}",
  "exit_code": null,
  "run_id": "app-server",
  "launcher_pid": 7671,
  "adopted": true,
  "backend": "proc",
  "uses_pty": false,
  "uses_pipes": false,
  "uses_dtach": false,
  "pty_mode": "raw",
  "runtime_id": "runtime-id",
  "signature": "signature",
  "app_id": "file_editor_cm6",
  "parent_shell_id": null,
  "is_app_worker": true
}}_worker": true
}}"#,
            stdout_log.display(),
            stderr_log.display()
        ),
    )
    .expect("write python record");

    let record = load_persisted_record(&record_path).expect("load python fws record");
    assert_eq!(record.id, "fs_python_record");
    assert_eq!(record.label, "app-worker:file_editor_cm6");
    assert_eq!(record.spec_id, "app:file_editor_cm6:app-worker");
    assert_eq!(record.backend, "proc");
    assert_eq!(record.pid, 99999999);
    assert_eq!(record.status, FerrousNativeShellStatus::Exited);
    assert_eq!(record.app_id.as_deref(), Some("file_editor_cm6"));
    assert!(record.is_app_worker);
    assert_eq!(
        record
            .env_overrides
            .get("TE_APP_WORKER_PORT")
            .map(String::as_str),
        Some("42401")
    );
    assert!(record.adopted);
}

#[test]
fn persisted_running_record_with_live_pid_stays_running() {
    let root = test_log_dir("live-persisted-record-read");
    let metadata_dir = root.join("meta").join("fs_live_record");
    fs::create_dir_all(&metadata_dir).expect("create metadata dir");
    let record_path = metadata_dir.join("meta.json");
    fs::write(
        &record_path,
        format!(
            r#"{{
  "id": "fs_live_record",
  "command": ["sh", "-c", "sleep 30"],
  "pid": {},
  "status": "running",
  "backend": "proc"
}}"#,
            std::process::id()
        ),
    )
    .expect("write live persisted record");

    let record = load_persisted_record(&record_path).expect("load live persisted record");
    assert_eq!(record.status, FerrousNativeShellStatus::Running);
    assert!(record.adopted);
}

#[test]
fn list_shells_caps_persisted_exited_history_to_fifty() {
    let manager = test_manager_with_store("persisted-exited-cap");
    for index in 0..55 {
        let shell_id = format!("fs_exited_{index:03}");
        let metadata_dir = manager.store().metadata_dir.join(&shell_id);
        fs::create_dir_all(&metadata_dir).expect("create metadata dir");
        fs::write(
            metadata_dir.join("meta.json"),
            format!(
                r#"{{
  "id": "{shell_id}",
  "command": ["sh", "-c", "true"],
  "pid": 99999999,
  "status": "exited",
  "backend": "proc",
  "created_at_ms": {index},
  "updated_at_ms": {index}
}}"#
            ),
        )
        .expect("write exited record");
    }

    let listed = manager.list_shells().expect("list persisted shells");
    let exited = listed
        .iter()
        .filter(|record| record.status == FerrousNativeShellStatus::Exited)
        .collect::<Vec<_>>();
    assert_eq!(exited.len(), 50);
    let ids = exited
        .iter()
        .map(|record| record.id.as_str())
        .collect::<Vec<_>>();
    assert!(!ids.contains(&"fs_exited_000"));
    assert!(!ids.contains(&"fs_exited_004"));
    assert!(ids.contains(&"fs_exited_005"));
    assert!(ids.contains(&"fs_exited_054"));
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
    assert!(!loaded.capabilities.stdin_eof);
    assert!(!loaded.capabilities.terminate);
    assert!(!loaded.capabilities.output_read);
    assert!(!loaded.capabilities.resize);
    assert!(loaded.capabilities.stdout_log);
    assert!(!loaded.capabilities.stdout_subscribe);
    assert!(!loaded.capabilities.stderr_subscribe);
    assert!(loaded.io_metadata_log.is_some());
    assert_eq!(loaded.runtime_id, Some(manager.store().runtime_id));
    assert_eq!(loaded.app_id, Some("tests".to_owned()));
    assert!(loaded.ui.is_empty());
    assert!(loaded.debug.is_empty());
    assert!(loaded.autostart);
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
fn lifecycle_subscription_reports_spawn_and_exit() {
    let manager = test_manager_with_store("lifecycle-subscription");
    let mut lifecycle = manager.subscribe_lifecycle();
    let record = manager
        .spawn_proc_blocking(base_proc_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "printf lifecycle".to_owned(),
            ],
            test_log_dir("lifecycle-subscription-proc"),
        ))
        .expect("spawn proc");

    let spawned = lifecycle.try_recv().expect("spawn lifecycle event");
    assert_eq!(spawned.kind, FerrousNativeLifecycleEventKind::Spawned);
    assert_eq!(spawned.shell_id, record.id);

    manager
        .wait_shell_blocking(&record.id, Duration::from_secs(3))
        .expect("wait shell")
        .expect("shell record");
    let started = Instant::now();
    loop {
        if let Ok(event) = lifecycle.try_recv() {
            if event.kind == FerrousNativeLifecycleEventKind::Exited {
                assert_eq!(event.shell_id, record.id);
                break;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "timed out waiting for exited lifecycle event"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn shutdown_tree_with_empty_roots_terminates_all_live_roots() {
    let manager = FerrousNativeManager::new();
    let first = manager
        .spawn_proc_blocking(base_proc_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            test_log_dir("shutdown-tree-all-first"),
        ))
        .expect("spawn first proc");
    let second = manager
        .spawn_proc_blocking(base_proc_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            test_log_dir("shutdown-tree-all-second"),
        ))
        .expect("spawn second proc");

    let result = manager
        .shutdown_tree_blocking(Vec::new())
        .expect("shutdown all roots through tree hook");
    assert!(result.ok, "shutdown errors: {:?}", result.stats.errors);
    assert_eq!(result.kind, "shutdown_tree");
    assert_eq!(
        result.root_pids,
        vec![i64::from(first.pid), i64::from(second.pid)]
    );
    assert_eq!(result.stats.total, 2);
    assert_eq!(result.stats.terminated, 2);

    for shell_id in [&first.id, &second.id] {
        let exited = manager
            .wait_shell_blocking(shell_id, Duration::from_secs(3))
            .expect("wait shell")
            .expect("shell record");
        assert_eq!(exited.status, FerrousNativeShellStatus::Exited);
    }
}

#[test]
fn shutdown_tree_kills_descendants_and_marks_adopted_python_record() {
    let manager = test_manager_with_store("shutdown-tree-mixed-python");
    let app_id = "mixed-python-app";
    let log_dir = test_log_dir("shutdown-tree-mixed-python-logs");
    let mut config = base_proc_config(
        vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "sleep 30 & printf 'child:%s\\n' \"$!\"; wait".to_owned(),
        ],
        log_dir,
    );
    config.label = format!("app-worker:{app_id}");
    config.spec_id = format!("app:{app_id}:worker");
    config.subgroups = vec![app_id.to_owned(), "app-worker".to_owned()];
    let root = manager.spawn_proc_blocking(config).expect("spawn root");
    let child_line = eventually_read_to_string(&root.stdout_log, Duration::from_secs(3));
    let child_pid = child_line
        .trim()
        .strip_prefix("child:")
        .expect("child pid line")
        .parse::<i64>()
        .expect("child pid");
    assert!(test_pid_is_live(child_pid), "child process should be live");

    let adopted_id = "fs_python_managed_child";
    let adopted_dir = manager.store().metadata_dir.join(adopted_id);
    fs::create_dir_all(&adopted_dir).expect("adopted record dir");
    let adopted_path = adopted_dir.join("meta.json");
    fs::write(
        &adopted_path,
        json!({
            "id": adopted_id,
            "backend": "proc",
            "pid": child_pid,
            "status": "running",
            "label": "python-managed-child",
            "subgroups": [app_id],
            "record_path": adopted_path.to_string_lossy(),
            "stdout_log": manager.store().logs_dir.join(format!("{adopted_id}.stdout.log")).to_string_lossy(),
            "stderr_log": manager.store().logs_dir.join(format!("{adopted_id}.stderr.log")).to_string_lossy(),
            "app_id": app_id,
            "created_at": 1.0,
            "updated_at": 1.0
        })
        .to_string(),
    )
    .expect("write adopted python record");

    let result = manager
        .shutdown_tree_blocking(vec![i64::from(root.pid)])
        .expect("shutdown mixed tree");
    assert!(result.ok, "shutdown errors: {:?}", result.stats.errors);
    assert!(
        result.stats.total >= 2,
        "tree plan should include root and child: {result:?}"
    );
    assert!(!test_pid_is_live(child_pid), "child process should exit");

    let adopted = load_persisted_record(&adopted_path).expect("load adopted record");
    assert_eq!(adopted.status, FerrousNativeShellStatus::Exited);
}

#[test]
fn shutdown_tree_with_roots_only_terminates_matching_live_roots() {
    let manager = FerrousNativeManager::new();
    let selected = manager
        .spawn_proc_blocking(base_proc_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            test_log_dir("shutdown-tree-selected"),
        ))
        .expect("spawn selected proc");
    let untouched = manager
        .spawn_proc_blocking(base_proc_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            test_log_dir("shutdown-tree-untouched"),
        ))
        .expect("spawn untouched proc");

    let result = manager
        .shutdown_tree_blocking(vec![i64::from(selected.pid)])
        .expect("shutdown selected root");
    assert!(result.ok, "shutdown errors: {:?}", result.stats.errors);
    assert_eq!(result.kind, "shutdown_tree");
    assert_eq!(result.target, selected.pid.to_string());
    assert_eq!(result.root_pids, vec![i64::from(selected.pid)]);
    assert_eq!(result.stats.total, 1);
    assert_eq!(result.stats.terminated, 1);

    let selected_exited = manager
        .wait_shell_blocking(&selected.id, Duration::from_secs(3))
        .expect("wait selected")
        .expect("selected shell record");
    assert_eq!(selected_exited.status, FerrousNativeShellStatus::Exited);

    let untouched_record = manager
        .get_shell(&untouched.id)
        .expect("get untouched")
        .expect("untouched record");
    assert_eq!(untouched_record.status, FerrousNativeShellStatus::Running);
    assert!(
        manager
            .terminate_shell_blocking(&untouched.id)
            .expect("terminate untouched")
    );
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
    assert!(record.capabilities.stdin_eof);
    assert!(record.capabilities.output_read);
    assert!(record.capabilities.stdout_subscribe);
    assert!(record.capabilities.stderr_subscribe);
    assert!(!record.capabilities.resize);
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
fn ferrous_framework_pipe_wrapper_exposes_blocking_line_contract() {
    let base_dir = test_log_dir("framework-pipe-base");
    let pipe = FerrousFrameworkPipe::spawn(FerrousPipeConfig {
        command: vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "while IFS= read -r line; do printf 'compat:%s\\n' \"$line\"; done".to_owned(),
        ],
        cwd: None,
        env: HashMap::from([
            (
                "FRAMEWORK_SHELLS_BASE_DIR".to_owned(),
                base_dir.to_string_lossy().into_owned(),
            ),
            (
                "FRAMEWORK_SHELLS_REPO_FINGERPRINT".to_owned(),
                "framework_pipe_wrapper".to_owned(),
            ),
            (
                "FRAMEWORK_SHELLS_SECRET".to_owned(),
                "framework-pipe-secret".to_owned(),
            ),
        ]),
        label: "framework-pipe".to_owned(),
        spec_id: "framework-pipe".to_owned(),
        subgroups: vec!["tests".to_owned()],
        log_dir: None,
        ..FerrousPipeConfig::default()
    })
    .expect("spawn wrapper pipe");

    assert!(!pipe.shell_id().expect("shell id").is_empty());
    pipe.write_line_blocking("hello").expect("write line");
    assert_eq!(
        pipe.read_line_blocking().expect("read line"),
        Some("compat:hello".to_owned())
    );
    pipe.close_blocking().expect("close pipe");
    assert_eq!(pipe.read_line_blocking().expect("read after close"), None);
}

#[test]
fn ferrous_framework_pipe_wrapper_loads_yaml_shellspec_and_merges_env() {
    let dir = test_log_dir("framework-pipe-yaml");
    let shellspec_path = dir.join("pipe.yaml");
    fs::write(
        &shellspec_path,
        r#"
version: "1"
shells:
  worker:
    backend: pipe
    command:
      - sh
      - -c
      - 'while IFS= read -r line; do printf "%s:%s:%s\n" "$APP_ID" "$BASE_ONLY" "$line"; done'
    env:
      APP_ID: "${ctx:APP_ID}"
    subgroups:
      - "${ctx:APP_ID}"
"#,
    )
    .expect("write shellspec");

    let pipe = FerrousFrameworkPipe::spawn(FerrousPipeConfig {
        command: vec!["python".to_owned()],
        cwd: Some(dir.clone()),
        env: HashMap::from([
            (
                "FRAMEWORK_SHELLS_BASE_DIR".to_owned(),
                dir.join("fws-base").to_string_lossy().into_owned(),
            ),
            (
                "FRAMEWORK_SHELLS_REPO_FINGERPRINT".to_owned(),
                "framework_pipe_yaml".to_owned(),
            ),
            (
                "FRAMEWORK_SHELLS_SECRET".to_owned(),
                "framework-pipe-yaml-secret".to_owned(),
            ),
            ("BASE_ONLY".to_owned(), "base-env".to_owned()),
        ]),
        label: "ignored-for-shellspec".to_owned(),
        spec_id: "worker".to_owned(),
        subgroups: vec!["fallback".to_owned()],
        shellspec_path: Some(shellspec_path),
        shellspec_entry: Some("worker".to_owned()),
        ctx: HashMap::from([("APP_ID".to_owned(), "ctx-app".to_owned())]),
        ..FerrousPipeConfig::default()
    })
    .expect("spawn wrapper shellspec pipe");

    pipe.write_line_blocking("ping").expect("write line");
    assert_eq!(
        pipe.read_line_blocking().expect("read line"),
        Some("ctx-app:base-env:ping".to_owned())
    );
    pipe.close_blocking().expect("close pipe");
}

#[test]
fn pipe_can_send_stdin_eof_and_clear_live_write_capabilities() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-eof");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "while IFS= read -r line; do printf 'ack:%s\n' \"$line\"; done; printf closed"
                    .to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pipe");

    manager
        .write_line_blocking(&record.id, "before-eof")
        .expect("write line");
    assert_eq!(
        manager
            .read_line_blocking(&record.id, Duration::from_secs(3))
            .expect("read line"),
        Some("ack:before-eof".to_owned())
    );
    assert!(
        manager
            .send_stdin_eof_blocking(&record.id)
            .expect("send eof")
    );
    let after_eof = manager
        .get_shell(&record.id)
        .expect("get shell")
        .expect("shell record");
    assert!(!after_eof.capabilities.stdin_write);
    assert!(!after_eof.capabilities.stdin_eof);
    let error = manager
        .write_line_blocking(&record.id, "after-eof")
        .expect_err("write after EOF should fail");
    assert!(error.to_string().contains("does not expose stdin"));
    let closed = manager
        .read_stdout_chunk_blocking(&record.id, Duration::from_secs(3))
        .expect("read closed chunk")
        .expect("closed chunk");
    assert!(String::from_utf8_lossy(&closed).contains("closed"));
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
    assert_eq!(record.pty_mode, Some(FerrousNativePtyMode::Interactive));
    assert!(record.capabilities.stdin_write);
    assert!(record.capabilities.stdin_eof);
    assert!(record.capabilities.output_read);
    assert!(record.capabilities.stdout_subscribe);
    assert!(!record.capabilities.stderr_subscribe);
    assert!(record.capabilities.resize);
    assert!(
        manager
            .resize_pty_blocking(&record.id, 100, 30)
            .expect("resize pty")
    );
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
fn pty_raw_mode_applies_raw_slave_termios() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pty-raw-mode");
    let mut config = base_pty_config(
        vec!["sh".to_owned(), "-c".to_owned(), "stty -a".to_owned()],
        log_dir,
    );
    config.mode = FerrousNativePtyMode::Raw;
    let record = manager.spawn_pty_blocking(config).expect("spawn raw pty");
    assert_eq!(record.pty_mode, Some(FerrousNativePtyMode::Raw));
    let output = read_chunks_until_contains(&manager, &record.id, "icanon", Duration::from_secs(3))
        .expect("read stty output")
        .expect("stty output");
    assert!(output.contains("-icanon"), "stty output: {output}");
    assert!(output.contains("-echo"), "stty output: {output}");
    let persisted: Value = serde_json::from_str(&eventually_read_to_string(
        &record.record_path,
        Duration::from_secs(3),
    ))
    .expect("persisted raw pty record");
    assert_eq!(
        persisted.get("pty_mode").and_then(Value::as_str),
        Some("raw")
    );
}

#[test]
fn pty_terminal_reports_request_response_metrics() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pty-terminal-bench");
    let record = manager
        .spawn_pty_blocking(base_pty_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "stty -echo; while IFS= read -r line; do printf 'pty-bench:%s\n' \"$line\"; done"
                    .to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pty");

    let request_count = 32;
    let bench_started = Instant::now();
    let mut latencies = Vec::new();
    for id in 1..=request_count {
        let request = format!("terminal-request-{id}");
        let started = Instant::now();
        manager
            .write_line_blocking(&record.id, &request)
            .expect("write pty request");
        let mut response = String::new();
        for _ in 0..8 {
            let Some(line) = manager
                .read_line_blocking(&record.id, Duration::from_secs(3))
                .expect("read pty response")
            else {
                continue;
            };
            if line.starts_with("pty-bench:") {
                response = line;
                break;
            }
        }
        latencies.push(started.elapsed());
        assert_eq!(response, format!("pty-bench:{request}"));
    }

    let total_elapsed = bench_started.elapsed();
    let stats = duration_stats(&latencies);
    eprintln!(
        "ferrous_native_pty_terminal_rr requests={} elapsed_ms={:.3} throughput_rps={:.1} min_ms={:.3} p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
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
    let error = manager
        .send_stdin_eof_blocking(&record.id)
        .expect_err("proc EOF must fail");
    assert!(error.to_string().contains("does not expose stdin EOF"));
    let error = manager
        .resize_pty_blocking(&record.id, 80, 24)
        .expect_err("proc resize must fail");
    assert!(error.to_string().contains("does not expose PTY resize"));
}

#[test]
fn proc_stdout_subscription_receives_reactor_chunks() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("proc-stdout-subscribe");
    let record = manager
        .spawn_proc_blocking(base_proc_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "sleep 0.1; printf subscribed-stdout".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn proc");
    let subscription = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stdout, 4)
        .expect("subscribe stdout")
        .expect("stdout subscription");
    let chunk = subscription
        .recv_timeout(Duration::from_secs(3))
        .expect("recv stdout chunk")
        .expect("stdout chunk");
    assert_eq!(chunk.shell_id, record.id);
    assert_eq!(chunk.stream, FerrousNativeOutputStream::Stdout);
    assert_eq!(String::from_utf8_lossy(&chunk.bytes), "subscribed-stdout");
    assert_eq!(chunk.dropped_before, 0);
}

#[test]
fn output_subscription_drops_slow_subscribers_when_full() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("subscription-backpressure");
    let record = manager
        .spawn_proc_blocking(base_proc_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "sleep 0.1; printf first; sleep 0.2; printf second; sleep 0.2; printf third"
                    .to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn proc");
    let subscription = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stdout, 1)
        .expect("subscribe stdout")
        .expect("stdout subscription");
    std::thread::sleep(Duration::from_millis(700));

    let chunk = subscription
        .recv_timeout(Duration::from_secs(1))
        .expect("recv first chunk")
        .expect("first chunk");
    assert_eq!(String::from_utf8_lossy(&chunk.bytes), "first");
    assert_eq!(
        subscription.try_recv().expect("try receive after drop"),
        None
    );
}

#[test]
fn pipe_stdout_subscription_receives_direct_read_chunks() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-stdout-subscribe");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "while IFS= read -r line; do printf 'sub:%s\n' \"$line\"; done".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pipe");
    let subscription = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stdout, 4)
        .expect("subscribe stdout")
        .expect("stdout subscription");
    manager
        .write_line_blocking(&record.id, "direct")
        .expect("write line");
    let line = manager
        .read_line_blocking(&record.id, Duration::from_secs(3))
        .expect("read line")
        .expect("line");
    assert_eq!(line, "sub:direct");
    let chunk = subscription
        .recv_timeout(Duration::from_secs(3))
        .expect("recv stdout chunk")
        .expect("stdout chunk");
    assert_eq!(chunk.stream, FerrousNativeOutputStream::Stdout);
    assert!(String::from_utf8_lossy(&chunk.bytes).contains("sub:direct"));
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn pipe_stderr_subscription_receives_reactor_chunks() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("pipe-stderr-subscribe");
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "while IFS= read -r line; do printf 'err:%s\n' \"$line\" >&2; done".to_owned(),
            ],
            log_dir,
        ))
        .expect("spawn pipe");
    let subscription = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stderr, 4)
        .expect("subscribe stderr")
        .expect("stderr subscription");
    manager
        .write_line_blocking(&record.id, "reactor")
        .expect("write line");
    let chunk = subscription
        .recv_timeout(Duration::from_secs(3))
        .expect("recv stderr chunk")
        .expect("stderr chunk");
    assert_eq!(chunk.stream, FerrousNativeOutputStream::Stderr);
    assert!(String::from_utf8_lossy(&chunk.bytes).contains("err:reactor"));
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
}

#[test]
fn subscription_rejects_unavailable_streams_and_zero_capacity() {
    let manager = FerrousNativeManager::new();
    let log_dir = test_log_dir("subscription-rejects");
    let record = manager
        .spawn_pty_blocking(base_pty_config(
            vec!["sh".to_owned(), "-c".to_owned(), "sleep 30".to_owned()],
            log_dir,
        ))
        .expect("spawn pty");
    let error = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stdout, 0)
        .err()
        .expect("zero capacity should fail");
    assert!(error.to_string().contains("capacity"));
    let error = manager
        .subscribe_output(&record.id, FerrousNativeOutputStream::Stderr, 4)
        .err()
        .expect("pty stderr subscription should fail");
    assert!(error.to_string().contains("subscription"));
    assert_eq!(
        manager
            .subscribe_output("missing-shell", FerrousNativeOutputStream::Stdout, 4)
            .expect("missing shell should not error")
            .is_none(),
        true
    );
    assert!(
        manager
            .terminate_shell_blocking(&record.id)
            .expect("terminate shell")
    );
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

#[test]
#[ignore = "performance benchmark; run explicitly with --ignored --nocapture"]
fn pipe_async_facade_reports_rtt_overhead_against_blocking_direct() {
    let request_count = 512;
    let payload_size = 512;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let blocking_first = run_blocking_pipe_rtt(request_count, payload_size);
    let async_second = runtime.block_on(run_async_facade_pipe_rtt(request_count, payload_size));
    report_pipe_overhead(
        "blocking_first",
        request_count,
        payload_size,
        blocking_first,
        async_second,
    );

    let async_first = runtime.block_on(run_async_facade_pipe_rtt(request_count, payload_size));
    let blocking_second = run_blocking_pipe_rtt(request_count, payload_size);
    report_pipe_overhead(
        "async_first",
        request_count,
        payload_size,
        blocking_second,
        async_first,
    );

    let blocking_avg_p50 = (blocking_first.stats.p50_ms + blocking_second.stats.p50_ms) / 2.0;
    let async_avg_p50 = (async_first.stats.p50_ms + async_second.stats.p50_ms) / 2.0;
    let blocking_avg_rps = (blocking_first.throughput_rps + blocking_second.throughput_rps) / 2.0;
    let async_avg_rps = (async_first.throughput_rps + async_second.throughput_rps) / 2.0;
    eprintln!(
        "ferrous_pipe_async_facade_overhead_summary requests={} payload_bytes={} blocking_avg_p50_ms={:.3} async_avg_p50_ms={:.3} avg_p50_delta_ms={:.3} blocking_avg_rps={:.1} async_avg_rps={:.1} avg_throughput_ratio={:.3}",
        request_count,
        payload_size,
        blocking_avg_p50,
        async_avg_p50,
        async_avg_p50 - blocking_avg_p50,
        blocking_avg_rps,
        async_avg_rps,
        async_avg_rps / blocking_avg_rps,
    );

    assert_eq!(blocking_first.responses, request_count);
    assert_eq!(blocking_second.responses, request_count);
    assert_eq!(async_first.responses, request_count);
    assert_eq!(async_second.responses, request_count);
}

#[test]
#[ignore = "performance benchmark; run explicitly with --ignored --nocapture"]
fn pipe_async_facade_reports_concurrent_inflight_metrics() {
    let request_count = 1024;
    let concurrency = 64;
    let payload_size = 512;
    let worker_threads = 4;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = runtime.block_on(run_async_facade_pipe_concurrent_rtt(
        request_count,
        concurrency,
        payload_size,
    ));
    eprintln!(
        "ferrous_pipe_async_facade_concurrent requests={} concurrency={} worker_threads={} payload_bytes={} elapsed_ms={:.3} throughput_rps={:.1} min_ms={:.3} p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        request_count,
        concurrency,
        worker_threads,
        payload_size,
        result.elapsed.as_secs_f64() * 1000.0,
        result.throughput_rps,
        result.stats.min_ms,
        result.stats.p50_ms,
        result.stats.p95_ms,
        result.stats.max_ms,
    );
    assert_eq!(result.responses, request_count);
}

#[derive(Clone, Copy, Debug, Default)]
struct DurationStats {
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct PipeBenchResult {
    responses: usize,
    elapsed: Duration,
    throughput_rps: f64,
    stats: DurationStats,
}

fn report_pipe_overhead(
    order: &str,
    request_count: usize,
    payload_size: usize,
    blocking: PipeBenchResult,
    async_facade: PipeBenchResult,
) {
    let p50_delta_ms = async_facade.stats.p50_ms - blocking.stats.p50_ms;
    let p95_delta_ms = async_facade.stats.p95_ms - blocking.stats.p95_ms;
    let throughput_ratio = async_facade.throughput_rps / blocking.throughput_rps;
    eprintln!(
        "ferrous_pipe_async_facade_overhead order={} requests={} payload_bytes={} blocking_elapsed_ms={:.3} blocking_rps={:.1} blocking_p50_ms={:.3} blocking_p95_ms={:.3} async_elapsed_ms={:.3} async_rps={:.1} async_p50_ms={:.3} async_p95_ms={:.3} p50_delta_ms={:.3} p95_delta_ms={:.3} throughput_ratio={:.3}",
        order,
        request_count,
        payload_size,
        blocking.elapsed.as_secs_f64() * 1000.0,
        blocking.throughput_rps,
        blocking.stats.p50_ms,
        blocking.stats.p95_ms,
        async_facade.elapsed.as_secs_f64() * 1000.0,
        async_facade.throughput_rps,
        async_facade.stats.p50_ms,
        async_facade.stats.p95_ms,
        p50_delta_ms,
        p95_delta_ms,
        throughput_ratio,
    );
}

fn run_blocking_pipe_rtt(request_count: usize, payload_size: usize) -> PipeBenchResult {
    let manager = FerrousNativeManager::new();
    let record = manager
        .spawn_pipe_blocking(base_pipe_config(
            jsonrpc_echo_command(),
            test_log_dir("pipe-blocking-rtt-overhead"),
        ))
        .expect("spawn blocking pipe");
    let result = run_pipe_rtt_loop(
        request_count,
        payload_size,
        |line| {
            manager
                .write_line_blocking(&record.id, line)
                .expect("blocking write request");
        },
        || {
            manager
                .read_line_blocking(&record.id, Duration::from_secs(3))
                .expect("blocking read line")
                .expect("blocking response line")
        },
    );
    manager
        .terminate_shell_blocking(&record.id)
        .expect("terminate blocking pipe");
    result
}

async fn run_async_facade_pipe_rtt(request_count: usize, payload_size: usize) -> PipeBenchResult {
    let manager = FerrousNativeManager::new();
    let record = manager
        .spawn_shell_pipe(base_pipe_config(
            jsonrpc_echo_command(),
            test_log_dir("pipe-async-rtt-overhead"),
        ))
        .await
        .expect("spawn async facade pipe");
    let mut latencies = Vec::with_capacity(request_count);
    let bench_started = Instant::now();
    for id in 1..=request_count {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "bench.echo",
            "params": {
                "echo": format!("overhead-{id}"),
                "push_count": 0,
                "payload_size": payload_size
            }
        });
        let started = Instant::now();
        manager
            .write_to_shell(&record.id, &request.to_string(), true)
            .await
            .expect("async facade write request");
        let line = {
            manager
                .read_line_blocking(&record.id, Duration::from_secs(3))
                .expect("async facade read line")
                .expect("async facade response line")
        };
        latencies.push(started.elapsed());
        assert_pipe_rtt_response(&line, id, payload_size);
    }
    let elapsed = bench_started.elapsed();
    let result = PipeBenchResult {
        responses: request_count,
        elapsed,
        throughput_rps: request_count as f64 / elapsed.as_secs_f64(),
        stats: duration_stats(&latencies),
    };
    manager
        .terminate_shell(&record.id, true)
        .await
        .expect("terminate async facade pipe");
    result
}

async fn run_async_facade_pipe_concurrent_rtt(
    request_count: usize,
    concurrency: usize,
    payload_size: usize,
) -> PipeBenchResult {
    let manager = FerrousNativeManager::new();
    let record = manager
        .spawn_shell_pipe(base_pipe_config(
            jsonrpc_echo_command(),
            test_log_dir("pipe-async-concurrent-rtt"),
        ))
        .await
        .expect("spawn async facade pipe");
    let shell_id = record.id.clone();
    let waiters = std::sync::Arc::new(std::sync::Mutex::new(HashMap::<
        usize,
        tokio::sync::oneshot::Sender<String>,
    >::new()));
    let reader_manager = manager.clone();
    let reader_shell_id = shell_id.clone();
    let reader_waiters = std::sync::Arc::clone(&waiters);
    let reader = tokio::task::spawn_blocking(move || {
        let mut received = 0_usize;
        while received < request_count {
            let line = reader_manager
                .read_line_blocking(&reader_shell_id, Duration::from_secs(5))
                .expect("concurrent read line")
                .expect("concurrent response line");
            let response: Value = serde_json::from_str(&line).expect("concurrent response json");
            let id = response
                .get("id")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .expect("concurrent response id");
            let waiter = reader_waiters
                .lock()
                .expect("waiter map lock")
                .remove(&id)
                .expect("response waiter");
            waiter.send(line).expect("send response to waiter");
            received += 1;
        }
        received
    });

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let bench_started = Instant::now();
    let mut handles = Vec::with_capacity(request_count);
    for id in 1..=request_count {
        let manager = manager.clone();
        let shell_id = shell_id.clone();
        let waiters = std::sync::Arc::clone(&waiters);
        let semaphore = std::sync::Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("semaphore permit");
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "bench.echo",
                "params": {
                    "echo": format!("concurrent-{id}"),
                    "push_count": 0,
                    "payload_size": payload_size
                }
            });
            let (tx, rx) = tokio::sync::oneshot::channel::<String>();
            waiters.lock().expect("waiter map lock").insert(id, tx);
            let started = Instant::now();
            manager
                .write_to_shell(&shell_id, &request.to_string(), true)
                .await
                .expect("concurrent async facade write request");
            let line = rx.await.expect("concurrent response");
            assert_pipe_rtt_response(&line, id, payload_size);
            started.elapsed()
        }));
    }
    let mut latencies = Vec::with_capacity(request_count);
    for handle in handles {
        latencies.push(handle.await.expect("request task"));
    }
    let received = reader.await.expect("reader task");
    assert_eq!(received, request_count);
    let elapsed = bench_started.elapsed();
    manager
        .terminate_shell(&record.id, true)
        .await
        .expect("terminate concurrent async facade pipe");
    PipeBenchResult {
        responses: request_count,
        elapsed,
        throughput_rps: request_count as f64 / elapsed.as_secs_f64(),
        stats: duration_stats(&latencies),
    }
}

fn run_pipe_rtt_loop(
    request_count: usize,
    payload_size: usize,
    mut write_request: impl FnMut(&str),
    mut read_response: impl FnMut() -> String,
) -> PipeBenchResult {
    let mut latencies = Vec::with_capacity(request_count);
    let bench_started = Instant::now();
    for id in 1..=request_count {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "bench.echo",
            "params": {
                "echo": format!("overhead-{id}"),
                "push_count": 0,
                "payload_size": payload_size
            }
        });
        let started = Instant::now();
        write_request(&request.to_string());
        let line = read_response();
        latencies.push(started.elapsed());

        assert_pipe_rtt_response(&line, id, payload_size);
    }
    let elapsed = bench_started.elapsed();
    PipeBenchResult {
        responses: request_count,
        elapsed,
        throughput_rps: request_count as f64 / elapsed.as_secs_f64(),
        stats: duration_stats(&latencies),
    }
}

fn assert_pipe_rtt_response(line: &str, id: usize, payload_size: usize) {
    let response: Value = serde_json::from_str(line).expect("response json");
    assert_eq!(response.get("id").and_then(Value::as_i64), Some(id as i64));
    assert_eq!(
        response
            .get("result")
            .and_then(|result| result.get("payload"))
            .and_then(Value::as_str)
            .map(str::len),
        Some(payload_size)
    );
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

fn read_chunks_until_contains(
    manager: &FerrousNativeManager,
    shell_id: &str,
    needle: &str,
    timeout: Duration,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut out = Vec::new();
    while started.elapsed() < timeout {
        if let Some(chunk) =
            manager.read_stdout_chunk_blocking(shell_id, Duration::from_millis(100))?
        {
            out.extend_from_slice(&chunk);
            let content = String::from_utf8_lossy(&out);
            if content.contains(needle) {
                return Ok(Some(content.into_owned()));
            }
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(String::from_utf8_lossy(&out).into_owned()))
    }
}

fn test_pid_is_live(pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    let stat_path = PathBuf::from(format!("/proc/{pid}/stat"));
    let Ok(stat) = fs::read_to_string(stat_path) else {
        return false;
    };
    let Some((_, after_comm)) = stat.rsplit_once(") ") else {
        return false;
    };
    let Some(state) = after_comm.chars().next() else {
        return false;
    };
    !matches!(state, 'Z' | 'X')
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
