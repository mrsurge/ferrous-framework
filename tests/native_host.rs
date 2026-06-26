use ferrous_framework::{
    FerrousNativeEnv, FerrousNativeHost, FerrousNativeHostConfig, FerrousNativeManager,
    FerrousNativePeer, FerrousNativePeerConfig, FerrousNativePipeConfig, FerrousNativeProcConfig,
    FerrousNativeStore, derive_native_api_token,
    peer_protocol::{
        FWS_DASHBOARD_OPEN_METHOD, FWS_LOGS_CHUNK_METHOD, FWS_LOGS_OPEN_METHOD,
        FWS_NOTIFICATION_EVENT, FWS_REQUEST_EVENT, FWS_SOCKETIO_NAMESPACE,
        FWS_SOCKETIO_SOCKET_PATH,
    },
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tf_rust_socketio::{ClientBuilder, Payload, TransportType, client::Client};

fn test_log_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let dir = std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("native-host-tests")
        .join(format!("{name}-{unique}"));
    fs::create_dir_all(&dir).expect("test log dir");
    dir
}

fn test_manager(name: &str) -> FerrousNativeManager {
    test_manager_with_secret(name, format!("host-secret-{name}"))
}

fn test_manager_with_secret(name: &str, secret: String) -> FerrousNativeManager {
    let store = FerrousNativeStore::from_base_dir_fingerprint_secret(
        test_log_dir(name).join("fws-base"),
        "host_test_fingerprint".to_owned(),
        secret.clone(),
    )
    .expect("test native store");
    FerrousNativeManager::with_store_and_env(
        store,
        FerrousNativeEnv {
            secret,
            run_id: format!("host-run-{name}"),
            fws_socketio_url: None,
            te_framework_url: None,
            extra: HashMap::new(),
        },
    )
}

fn test_child_manager_with_parent_url(
    name: &str,
    secret: String,
    parent_url: String,
) -> FerrousNativeManager {
    let store = FerrousNativeStore::from_base_dir_fingerprint_secret(
        test_log_dir(name).join("fws-base"),
        "host_test_fingerprint".to_owned(),
        secret.clone(),
    )
    .expect("test native store");
    FerrousNativeManager::with_store_and_env(
        store,
        FerrousNativeEnv {
            secret,
            run_id: format!("host-run-{name}"),
            fws_socketio_url: Some(parent_url.clone()),
            te_framework_url: Some(parent_url),
            extra: HashMap::from([("FRAMEWORK_SHELLS_FWS_CHILD".to_owned(), "1".to_owned())]),
        },
    )
}

#[test]
fn native_host_serves_control_plane_and_shell_io() {
    let manager = test_manager("control-plane");
    let token = derive_native_api_token(&manager.native_env().secret);
    let host = FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), manager)
        .expect("spawn native host");
    let addr = host.addr();

    let (status, body) = request(addr, "GET", "/health", &[], "");
    assert_eq!(status, 200);
    assert_eq!(json_body(&body)["data"]["backend"], "ferrous-native");

    let (status, _body) = request(addr, "GET", "/fws", &[], "");
    assert_eq!(status, 308);

    let (status, body) = request(addr, "GET", "/fws/", &[], "");
    assert_eq!(status, 200);
    assert!(body.contains("Framework Shells"));
    assert!(body.contains("/fws/static/fws.css"));
    assert!(body.contains("/fws/static/fws.js"));

    let (status, body) = request(addr, "GET", "/fws/static/fws.js", &[], "");
    assert_eq!(status, 200);
    assert!(body.contains("fws.dashboard.open"));
    assert!(body.contains("/fws_ws/socket.io"));

    let (status, body) = request(addr, "GET", "/static/vendor/socket.io.min.js", &[], "");
    assert_eq!(status, 200);
    assert!(body.contains("socket.io"));

    let (status, body) = request(addr, "GET", "/api/framework_shells/runtime", &[], "");
    assert_eq!(status, 200);
    let runtime = json_body(&body);
    assert_eq!(runtime["data"]["socketio"], true);
    assert_eq!(runtime["data"]["socketio_namespace"], "/fws");
    assert_eq!(runtime["data"]["socketio_path"], "/fws_ws/socket.io");
    assert_eq!(runtime["data"]["peer_count"], 0);

    let env = host.child_env_overlay();
    assert_eq!(env.get("TE_FRAMEWORK_URL"), Some(&host.url()));
    assert_eq!(
        env.get("FRAMEWORK_SHELLS_FWS_SOCKETIO_URL"),
        Some(&host.url())
    );
    assert_eq!(env.get("FRAMEWORK_SHELLS_FWS_CHILD"), Some(&"1".to_owned()));
    assert!(env.contains_key("FRAMEWORK_SHELLS_SECRET"));

    let create = json!({
        "backend": "pipe",
        "command": ["sh", "-c", "while IFS= read -r line; do printf 'ack:%s\\n' \"$line\"; done"],
        "label": "host-pipe",
        "spec_id": "host-pipe",
        "subgroups": ["host-tests"]
    })
    .to_string();
    let (status, body) = request(addr, "POST", "/api/framework_shells", &[], &create);
    assert_eq!(status, 403, "body: {body}");

    let (status, body) = request(
        addr,
        "POST",
        "/api/framework_shells",
        &[("X-Framework-Key", token.as_str())],
        &create,
    );
    assert_eq!(status, 200, "body: {body}");
    let created = json_body(&body);
    let shell_id = created["data"]["id"].as_str().expect("shell id").to_owned();
    assert_eq!(created["data"]["backend"], "pipe");
    assert_eq!(created["data"]["capabilities"]["stdin_write"], true);

    let input = json!({ "data": "ping-host", "append_newline": true }).to_string();
    let (status, body) = request(
        addr,
        "POST",
        &format!("/api/framework_shells/{shell_id}/input"),
        &[("X-Framework-Key", token.as_str())],
        &input,
    );
    assert_eq!(status, 200, "body: {body}");

    let (status, body) = request(
        addr,
        "GET",
        &format!(
            "/api/framework_shells/logs/{shell_id}/tail?stream=stdout&bytes=4096&drain_timeout_ms=250"
        ),
        &[],
        "",
    );
    assert_eq!(status, 200, "body: {body}");
    assert!(
        json_body(&body)["data"]["stdout"]["text"]
            .as_str()
            .expect("stdout text")
            .contains("ack:ping-host")
    );

    let (status, body) = request(
        addr,
        "POST",
        "/api/framework_shells/app/host-tests/shutdown",
        &[("X-Framework-Key", token.as_str())],
        "",
    );
    assert_eq!(status, 200, "body: {body}");
    let shutdown = json_body(&body);
    assert_eq!(shutdown["data"]["kind"], "shutdown_group");
    assert_eq!(shutdown["data"]["stats"]["total"], 1);
    assert_eq!(shutdown["data"]["stats"]["terminated"], 1);
    assert_eq!(shutdown["data"]["stats"]["clean_exits"], 1);
    assert_eq!(shutdown["data"]["stats"]["force_killed"], 0);

    host.close_blocking().expect("close host");
}

#[test]
fn native_host_exposes_framework_shutdown_route() {
    let manager = test_manager("shutdown-route");
    let token = derive_native_api_token(&manager.native_env().secret);
    let host = FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), manager)
        .expect("spawn native host");
    let addr = host.addr();

    let create = json!({
        "backend": "proc",
        "command": ["sh", "-c", "sleep 30"],
        "label": "shutdown-route-proc",
        "spec_id": "shutdown-route-proc",
        "subgroups": ["shutdown-route-tests"]
    })
    .to_string();
    let (status, body) = request(
        addr,
        "POST",
        "/api/framework_shells",
        &[("X-Framework-Key", token.as_str())],
        &create,
    );
    assert_eq!(status, 200, "body: {body}");

    let (status, body) = request(
        addr,
        "POST",
        "/api/framework_shells/shutdown",
        &[("X-Framework-Key", token.as_str())],
        &json!({"scope": "all"}).to_string(),
    );
    assert_eq!(status, 200, "body: {body}");
    let shutdown = json_body(&body);
    assert_eq!(shutdown["data"]["kind"], "shutdown_all");
    assert_eq!(shutdown["data"]["target"], "all");
    assert_eq!(shutdown["data"]["stats"]["total"], 1);
    assert_eq!(shutdown["data"]["stats"]["terminated"], 1);

    host.close_blocking().expect("close host");
}

#[test]
fn native_host_routes_shell_input_to_ferrous_peer() {
    let secret = "ferrous-peer-shared-secret".to_owned();
    let host_manager = test_manager_with_secret("peer-controller", secret.clone());
    let peer_manager = test_manager_with_secret("peer-owner", secret.clone());
    let token = derive_native_api_token(&secret);
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), host_manager)
            .expect("spawn native host");
    let addr = host.addr();

    let peer = FerrousNativePeer::connect(
        peer_manager.clone(),
        FerrousNativePeerConfig::new(host.url()),
    )
    .expect("connect ferrous peer");
    wait_for_peer_count(addr, 1);

    let shell = peer_manager
        .spawn_pipe_blocking(FerrousNativePipeConfig {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while IFS= read -r line; do printf 'peer-ack:%s\\n' \"$line\"; done".into(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "peer-pipe".into(),
            spec_id: "peer-pipe".into(),
            subgroups: vec!["peer-tests".into()],
            log_dir: None,
        })
        .expect("spawn peer pipe");

    let input = json!({ "data": "from-controller", "append_newline": true }).to_string();
    let (status, body) = request(
        addr,
        "POST",
        &format!("/api/framework_shells/{}/input", shell.id),
        &[("X-Framework-Key", token.as_str())],
        &input,
    );
    assert_eq!(status, 200, "body: {body}");
    let response = json_body(&body);
    assert_eq!(response["data"]["accepted"], true);
    assert_eq!(response["data"]["shell_id"], shell.id);

    let line = peer_manager
        .read_line_blocking(&shell.id, Duration::from_secs(3))
        .expect("read peer shell line")
        .expect("peer shell response");
    assert!(
        line.contains("peer-ack:from-controller"),
        "unexpected peer shell line: {line:?}"
    );

    peer.disconnect().expect("disconnect peer");
    let _ = peer_manager.terminate_shell_strict_blocking(&shell.id, true);
    host.close_blocking().expect("close host");
}

#[test]
fn native_peer_relays_lifecycle_notifications_to_controller() {
    let secret = "ferrous-peer-notification-secret".to_owned();
    let host_manager = test_manager_with_secret("peer-notify-controller", secret.clone());
    let peer_manager = test_manager_with_secret("peer-notify-owner", secret.clone());
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), host_manager)
            .expect("spawn native host");
    let addr = host.addr();

    let peer = FerrousNativePeer::connect(
        peer_manager.clone(),
        FerrousNativePeerConfig::new(host.url()),
    )
    .expect("connect ferrous peer");
    wait_for_peer_count(addr, 1);
    let baseline = peer_notifications_received(addr);

    let shell = peer_manager
        .spawn_proc_blocking(FerrousNativeProcConfig {
            command: vec!["sh".into(), "-c".into(), "sleep 0.2".into()],
            cwd: None,
            env: HashMap::new(),
            label: "peer-notify-proc".into(),
            spec_id: "peer-notify-proc".into(),
            subgroups: vec!["peer-tests".into()],
            log_dir: None,
        })
        .expect("spawn peer proc");

    wait_for_peer_notifications(addr, baseline + 1);
    let _ = peer_manager.terminate_shell_strict_blocking(&shell.id, true);
    peer.disconnect().expect("disconnect peer");
    host.close_blocking().expect("close host");
}

#[test]
fn native_dashboard_receives_local_shell_lifecycle_notifications() {
    let manager = test_manager("local-dashboard-lifecycle");
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), manager.clone())
            .expect("spawn native host");
    let (browser, notifications) = connect_dashboard_browser(&host);

    let shell = manager
        .spawn_proc_blocking(FerrousNativeProcConfig {
            command: vec!["sh".into(), "-c".into(), "sleep 5".into()],
            cwd: None,
            env: HashMap::new(),
            label: "local-dashboard-proc".into(),
            spec_id: "local-dashboard-proc".into(),
            subgroups: vec!["dashboard-tests".into()],
            log_dir: None,
        })
        .expect("spawn local proc");

    wait_for_shell_lifecycle_notification(&notifications, "fws.shell.spawned", &shell.id);
    manager
        .terminate_shell_strict_blocking(&shell.id, true)
        .expect("terminate local proc");
    wait_for_shell_lifecycle_notification(&notifications, "fws.shell.exited", &shell.id);

    browser.disconnect().expect("disconnect browser");
    host.close_blocking().expect("close host");
}

#[test]
fn native_dashboard_receives_peer_shell_lifecycle_notifications() {
    let secret = "ferrous-peer-dashboard-lifecycle-secret".to_owned();
    let host_manager = test_manager_with_secret("peer-dashboard-controller", secret.clone());
    let peer_manager = test_manager_with_secret("peer-dashboard-owner", secret.clone());
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), host_manager)
            .expect("spawn native host");
    let addr = host.addr();
    let peer = FerrousNativePeer::connect(
        peer_manager.clone(),
        FerrousNativePeerConfig::new(host.url()),
    )
    .expect("connect ferrous peer");
    wait_for_peer_count(addr, 1);
    let (browser, notifications) = connect_dashboard_browser(&host);

    let shell = peer_manager
        .spawn_proc_blocking(FerrousNativeProcConfig {
            command: vec!["sh".into(), "-c".into(), "sleep 5".into()],
            cwd: None,
            env: HashMap::new(),
            label: "peer-dashboard-proc".into(),
            spec_id: "peer-dashboard-proc".into(),
            subgroups: vec!["dashboard-tests".into()],
            log_dir: None,
        })
        .expect("spawn peer proc");

    wait_for_shell_lifecycle_notification(&notifications, "fws.shell.spawned", &shell.id);
    peer_manager
        .terminate_shell_strict_blocking(&shell.id, true)
        .expect("terminate peer proc");
    wait_for_shell_lifecycle_notification(&notifications, "fws.shell.exited", &shell.id);

    browser.disconnect().expect("disconnect browser");
    peer.disconnect().expect("disconnect peer");
    host.close_blocking().expect("close host");
}

#[test]
fn native_child_manager_auto_peer_relays_lifecycle_and_logs_to_parent() {
    let secret = "ferrous-auto-peer-dashboard-secret".to_owned();
    let host_manager = test_manager_with_secret("auto-peer-controller", secret.clone());
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), host_manager)
            .expect("spawn native host");
    let addr = host.addr();
    let child_manager = test_child_manager_with_parent_url("auto-peer-child", secret, host.url());
    wait_for_peer_count(addr, 1);
    let (browser, notifications) = connect_dashboard_browser(&host);

    let shell = child_manager
        .spawn_pipe_blocking(FerrousNativePipeConfig {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while IFS= read -r line; do printf 'auto-peer-log:%s\\n' \"$line\"; done".into(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "auto-peer-dashboard-pipe".into(),
            spec_id: "auto-peer-dashboard-pipe".into(),
            subgroups: vec!["dashboard-tests".into()],
            log_dir: None,
        })
        .expect("spawn auto-peer child pipe");

    wait_for_shell_lifecycle_notification(&notifications, "fws.shell.spawned", &shell.id);
    open_shell_logs(&browser, &shell.id);
    child_manager
        .write_line_blocking(&shell.id, "probe")
        .expect("write auto-peer child pipe");
    let line = child_manager
        .read_line_blocking(&shell.id, Duration::from_secs(3))
        .expect("read auto-peer child pipe")
        .expect("auto-peer child pipe response");
    assert!(line.contains("auto-peer-log:probe"));
    wait_for_log_chunk_notification(&notifications, &shell.id, "auto-peer-log:probe");

    browser.disconnect().expect("disconnect browser");
    let _ = child_manager.terminate_shell_strict_blocking(&shell.id, true);
    host.close_blocking().expect("close host");
}

#[test]
fn native_child_manager_auto_peer_can_start_inside_tokio_runtime() {
    let secret = "ferrous-auto-peer-tokio-secret".to_owned();
    let host_manager = test_manager_with_secret("auto-peer-tokio-controller", secret.clone());
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), host_manager)
            .expect("spawn native host");
    let addr = host.addr();
    let host_url = host.url();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let child_manager = runtime.block_on(async move {
        test_child_manager_with_parent_url("auto-peer-tokio-child", secret, host_url)
    });
    wait_for_peer_count(addr, 1);
    drop(child_manager);
    host.close_blocking().expect("close host");
}

#[test]
fn native_dashboard_receives_local_pipe_log_chunks() {
    let manager = test_manager("local-dashboard-logs");
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), manager.clone())
            .expect("spawn native host");
    let (browser, notifications) = connect_dashboard_browser(&host);

    let shell = manager
        .spawn_pipe_blocking(FerrousNativePipeConfig {
            command: vec![
                "sh".into(),
                "-c".into(),
                "while IFS= read -r line; do printf 'dashboard-log:%s\\n' \"$line\"; done".into(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "local-dashboard-pipe".into(),
            spec_id: "local-dashboard-pipe".into(),
            subgroups: vec!["dashboard-tests".into()],
            log_dir: None,
        })
        .expect("spawn local pipe");
    open_shell_logs(&browser, &shell.id);

    manager
        .write_line_blocking(&shell.id, "probe")
        .expect("write local pipe");
    let line = manager
        .read_line_blocking(&shell.id, Duration::from_secs(3))
        .expect("read local pipe")
        .expect("local pipe response");
    assert!(line.contains("dashboard-log:probe"));
    wait_for_log_chunk_notification(&notifications, &shell.id, "dashboard-log:probe");

    browser.disconnect().expect("disconnect browser");
    let _ = manager.terminate_shell_strict_blocking(&shell.id, true);
    host.close_blocking().expect("close host");
}

#[test]
fn native_dashboard_receives_local_proc_log_chunks() {
    let manager = test_manager("local-dashboard-proc-logs");
    let host =
        FerrousNativeHost::spawn_with_manager(FerrousNativeHostConfig::default(), manager.clone())
            .expect("spawn native host");
    let (browser, notifications) = connect_dashboard_browser(&host);

    let shell = manager
        .spawn_proc_blocking(FerrousNativeProcConfig {
            command: vec![
                "sh".into(),
                "-c".into(),
                "sleep 0.5; printf 'dashboard-proc-log\\n'; sleep 1".into(),
            ],
            cwd: None,
            env: HashMap::new(),
            label: "local-dashboard-proc-log".into(),
            spec_id: "local-dashboard-proc-log".into(),
            subgroups: vec!["dashboard-tests".into()],
            log_dir: None,
        })
        .expect("spawn local proc");
    open_shell_logs(&browser, &shell.id);

    wait_for_log_chunk_notification(&notifications, &shell.id, "dashboard-proc-log");

    browser.disconnect().expect("disconnect browser");
    let _ = manager.terminate_shell_strict_blocking(&shell.id, true);
    host.close_blocking().expect("close host");
}

fn wait_for_peer_count(addr: SocketAddr, expected: usize) {
    for _ in 0..50 {
        let (status, body) = request(addr, "GET", "/api/framework_shells/runtime", &[], "");
        if status == 200
            && json_body(&body)["data"]["peer_count"]
                .as_u64()
                .map(|count| count as usize)
                == Some(expected)
        {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for peer_count={expected}");
}

fn peer_notifications_received(addr: SocketAddr) -> u64 {
    let (status, body) = request(addr, "GET", "/api/framework_shells/runtime", &[], "");
    assert_eq!(status, 200, "body: {body}");
    json_body(&body)["data"]["peer_notifications_received"]
        .as_u64()
        .unwrap_or_default()
}

fn wait_for_peer_notifications(addr: SocketAddr, expected_at_least: u64) {
    for _ in 0..50 {
        let count = peer_notifications_received(addr);
        if count >= expected_at_least {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for peer_notifications_received >= {expected_at_least}");
}

fn connect_dashboard_browser(host: &FerrousNativeHost) -> (Client, Receiver<Value>) {
    let (notification_tx, notification_rx) = mpsc::channel::<Value>();
    let client = ClientBuilder::new(socketio_url(&host.url()))
        .namespace(FWS_SOCKETIO_NAMESPACE)
        .transport_type(TransportType::Websocket)
        .auth(json!({ "role": "browser" }))
        .on(FWS_NOTIFICATION_EVENT, move |payload, _socket| {
            if let Some(value) = payload_value(&payload) {
                let _ = notification_tx.send(value);
            }
        })
        .connect()
        .expect("connect dashboard browser");
    thread::sleep(Duration::from_millis(100));
    emit_dashboard_request(
        &client,
        FWS_DASHBOARD_OPEN_METHOD,
        json!({ "view": "html" }),
        "dashboard-open",
    );
    (client, notification_rx)
}

fn open_shell_logs(client: &Client, shell_id: &str) {
    emit_dashboard_request(
        client,
        FWS_LOGS_OPEN_METHOD,
        json!({ "shell_id": shell_id }),
        "logs-open",
    );
}

fn emit_dashboard_request(client: &Client, method: &str, params: Value, id: &str) {
    let mut last_error = None;
    let mut ack_rx_result = None;
    for _ in 0..40 {
        let (ack_tx, ack_rx) = mpsc::channel::<Value>();
        match client.emit_with_ack(
            FWS_REQUEST_EVENT,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }),
            Duration::from_secs(3),
            move |payload, _socket| {
                if let Some(value) = payload_value(&payload) {
                    let _ = ack_tx.send(value);
                }
            },
        ) {
            Ok(()) => {
                ack_rx_result = Some(ack_rx);
                break;
            }
            Err(error) => {
                last_error = Some(error.to_string());
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
    let ack_rx = ack_rx_result
        .unwrap_or_else(|| panic!("emit dashboard request failed for {method}: {last_error:?}"));
    let ack = ack_rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap_or_else(|error| panic!("{method} ack timeout: {error}; emit_error={last_error:?}"));
    assert!(
        ack.get("result").is_some() || ack.get("error").is_some(),
        "unexpected {method} ack: {ack}"
    );
    assert!(
        ack.get("error").is_none(),
        "unexpected {method} error ack: {ack}"
    );
}

fn wait_for_shell_lifecycle_notification(
    notifications: &Receiver<Value>,
    method: &str,
    shell_id: &str,
) -> Value {
    wait_for_notification(notifications, |notification| {
        notification.get("method").and_then(Value::as_str) == Some(method)
            && notification
                .get("params")
                .and_then(Value::as_object)
                .and_then(|params| params.get("shell"))
                .and_then(Value::as_object)
                .and_then(|shell| shell.get("id"))
                .and_then(Value::as_str)
                == Some(shell_id)
    })
}

fn wait_for_log_chunk_notification(
    notifications: &Receiver<Value>,
    shell_id: &str,
    expected_text: &str,
) -> Value {
    wait_for_notification(notifications, |notification| {
        notification.get("method").and_then(Value::as_str) == Some(FWS_LOGS_CHUNK_METHOD)
            && notification
                .get("params")
                .and_then(Value::as_object)
                .and_then(|params| params.get("shell_id"))
                .and_then(Value::as_str)
                == Some(shell_id)
            && notification
                .get("params")
                .and_then(Value::as_object)
                .and_then(|params| params.get("chunk"))
                .and_then(Value::as_str)
                .is_some_and(|chunk| chunk.contains(expected_text))
    })
}

fn wait_for_notification(
    notifications: &Receiver<Value>,
    mut predicate: impl FnMut(&Value) -> bool,
) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let timeout = remaining.min(Duration::from_millis(200));
        match notifications.recv_timeout(timeout) {
            Ok(notification) if predicate(&notification) => return notification,
            Ok(_) => continue,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    panic!("timed out waiting for dashboard notification");
}

fn socketio_url(base_url: &str) -> String {
    format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        FWS_SOCKETIO_SOCKET_PATH
    )
}

fn payload_value(payload: &Payload) -> Option<Value> {
    #[allow(deprecated)]
    let value = match payload {
        Payload::Text(values, _) => values.first().cloned(),
        Payload::String(value, _) => serde_json::from_str(value).ok(),
        Payload::Binary(_, _) => None,
    }?;
    Some(unwrap_socketio_payload_value(value))
}

fn unwrap_socketio_payload_value(value: Value) -> Value {
    if let Some(values) = value.as_array()
        && let Some(first) = values.first()
    {
        return first.clone();
    }
    value
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut last_error = None;
    for _ in 0..50 {
        match try_request(addr, method, path, headers, body) {
            Ok(response) => return response,
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    panic!("request failed: {:?}", last_error);
}

fn try_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_default();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

fn json_body(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|error| panic!("invalid json body {body:?}: {error}"))
}
