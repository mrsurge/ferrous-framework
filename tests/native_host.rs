use ferrous_framework::{
    FerrousNativeEnv, FerrousNativeHost, FerrousNativeHostConfig, FerrousNativeManager,
    FerrousNativeStore, derive_native_api_token,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    thread,
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
        .join("native-host-tests")
        .join(format!("{name}-{unique}"));
    fs::create_dir_all(&dir).expect("test log dir");
    dir
}

fn test_manager(name: &str) -> FerrousNativeManager {
    let secret = format!("host-secret-{name}");
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

    let env = host.child_env_overlay();
    assert_eq!(env.get("TE_FRAMEWORK_URL"), Some(&host.url()));
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
    assert_eq!(shutdown["data"]["stats"]["force_killed"], 1);

    host.close_blocking().expect("close host");
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
