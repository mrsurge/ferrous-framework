use crate::native_runtime::{
    FerrousNativeManager, FerrousNativePipeConfig, FerrousNativeProcConfig, FerrousNativePtyConfig,
    FerrousNativePtyMode, FerrousNativeShellRecord, FerrousNativeShellStatus, FerrousNativeStore,
};
use crate::shellspec::ShellspecRenderInput;
use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    net::{SocketAddr, TcpListener, ToSocketAddrs},
    path::{Path as FsPath, PathBuf},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::oneshot;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeHostConfig {
    pub host: String,
    pub port: u16,
    pub require_auth: bool,
}

impl Default for FerrousNativeHostConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 0,
            require_auth: true,
        }
    }
}

pub struct FerrousNativeHost {
    manager: FerrousNativeManager,
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<Result<()>>>,
}

#[derive(Clone)]
struct HostState {
    manager: FerrousNativeManager,
    api_token: String,
    require_auth: bool,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct ApiEnvelope<T> {
    ok: bool,
    data: T,
}

#[derive(Debug, Serialize)]
struct ApiErrorEnvelope {
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize)]
struct HostInfoPayload {
    backend: &'static str,
    api_compat: &'static str,
    transport_owner: &'static str,
    python_runtime: bool,
    socketio: bool,
    url: String,
    store: StorePayload,
}

#[derive(Debug, Serialize)]
struct StorePayload {
    runtime_id: String,
    repo_fingerprint: String,
    root: String,
    logs_dir: String,
    metadata_dir: String,
    sockets_dir: String,
}

#[derive(Debug, Serialize)]
struct ShellPayload {
    id: String,
    spec_id: String,
    backend: String,
    command: Vec<String>,
    cwd: Option<String>,
    pid: u32,
    status: FerrousNativeShellStatus,
    exit_code: Option<i32>,
    label: String,
    subgroups: Vec<String>,
    record_path: String,
    stdout_log: String,
    stderr_log: String,
    io_metadata_log: Option<String>,
    pty_mode: Option<FerrousNativePtyMode>,
    autostart: bool,
    ui: Value,
    debug: Value,
    runtime_id: Option<String>,
    app_id: Option<String>,
    parent_shell_id: Option<String>,
    is_app_worker: bool,
    capabilities: crate::native_runtime::FerrousNativeShellCapabilities,
    adopted: bool,
    created_at: f64,
    updated_at: f64,
    created_at_ms: u128,
    updated_at_ms: u128,
    env_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CreateShellRequest {
    command: Vec<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    spec_id: Option<String>,
    #[serde(default)]
    subgroups: Vec<String>,
    #[serde(default)]
    log_dir: Option<String>,
    #[serde(default)]
    pty_mode: Option<FerrousNativePtyMode>,
}

#[derive(Debug, Deserialize)]
struct ShellInputRequest {
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    append_newline: bool,
    #[serde(default)]
    eof: bool,
}

#[derive(Debug, Deserialize)]
struct ShellActionRequest {
    action: String,
}

#[derive(Debug, Deserialize)]
struct ShellspecApplyRequest {
    document: Value,
    #[serde(default)]
    ctx: HashMap<String, String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    prune: bool,
}

#[derive(Debug, Deserialize)]
struct TailQuery {
    #[serde(default = "default_tail_stream")]
    stream: String,
    #[serde(default = "default_tail_bytes")]
    bytes: usize,
    #[serde(default = "default_drain_timeout_ms")]
    drain_timeout_ms: u64,
}

#[derive(Debug, Serialize)]
struct LogTailPayload {
    shell_id: String,
    stdout: Option<LogTailStreamPayload>,
    stderr: Option<LogTailStreamPayload>,
}

#[derive(Debug, Serialize)]
struct LogTailStreamPayload {
    path: String,
    text: String,
    byte_window_start: u64,
    byte_window_end: u64,
    partial_head: bool,
    truncated: bool,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl FerrousNativeHost {
    pub fn spawn(config: FerrousNativeHostConfig) -> Result<Self> {
        let listener = bind_listener(&config)?;
        let addr = listener
            .local_addr()
            .context("failed to read host address")?;
        let store = FerrousNativeStore::from_process_env()?;
        let mut native_env = crate::native_runtime::FerrousNativeEnv::from_process_env_with_secret(
            store.secret.clone(),
        );
        if native_env.te_framework_url.is_none() {
            native_env.te_framework_url = Some(url_for_addr(addr));
        }
        let manager = FerrousNativeManager::with_store_and_env(store, native_env);
        Self::spawn_with_listener(config, manager, listener, addr)
    }

    pub fn spawn_with_manager(
        config: FerrousNativeHostConfig,
        manager: FerrousNativeManager,
    ) -> Result<Self> {
        let listener = bind_listener(&config)?;
        let addr = listener
            .local_addr()
            .context("failed to read host address")?;
        Self::spawn_with_listener(config, manager, listener, addr)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn url(&self) -> String {
        url_for_addr(self.addr)
    }

    pub fn manager(&self) -> FerrousNativeManager {
        self.manager.clone()
    }

    pub fn child_env_overlay(&self) -> HashMap<String, String> {
        let mut env = self.manager.child_env_overlay();
        env.entry("TE_FRAMEWORK_URL".to_owned())
            .or_insert_with(|| self.url());
        env
    }

    pub fn close_blocking(mut self) -> Result<()> {
        self.shutdown()
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| anyhow!("native host thread panicked"))??;
        }
        Ok(())
    }

    fn spawn_with_listener(
        config: FerrousNativeHostConfig,
        manager: FerrousNativeManager,
        listener: TcpListener,
        addr: SocketAddr,
    ) -> Result<Self> {
        listener
            .set_nonblocking(true)
            .context("failed to configure native host listener")?;
        let state = HostState {
            api_token: derive_api_token(&manager.native_env().secret),
            manager: manager.clone(),
            require_auth: config.require_auth,
            base_url: url_for_addr(addr),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = thread::spawn(move || run_host_thread(listener, state, shutdown_rx));
        Ok(Self {
            manager,
            addr,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        })
    }
}

impl Drop for FerrousNativeHost {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

pub fn derive_api_token(secret: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts secret keys of any length");
    mac.update(b"api");
    hex_encode(&mac.finalize().into_bytes())
}

fn run_host_thread(
    listener: TcpListener,
    state: HostState,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build native host tokio runtime")?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)
            .context("failed to attach native host listener to tokio")?;
        axum::serve(listener, router(state))
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .context("native host server failed")
    })
}

fn router(state: HostState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/fws", get(dashboard))
        .route("/fws/", get(dashboard))
        .route("/api/framework_shells/runtime", get(runtime_info))
        .route("/api/framework_shells", get(list_shells).post(create_shell))
        .route(
            "/api/framework_shells/shellspec/apply",
            post(apply_shellspec),
        )
        .route("/api/framework_shells/{shell_id}", get(get_shell))
        .route(
            "/api/framework_shells/{shell_id}/terminate",
            post(terminate_shell),
        )
        .route(
            "/api/framework_shells/{shell_id}/action",
            post(shell_action),
        )
        .route("/api/framework_shells/{shell_id}/input", post(shell_input))
        .route(
            "/api/framework_shells/app/{app_id}/shutdown",
            post(shutdown_app_group),
        )
        .route("/api/framework_shells/logs/{shell_id}/tail", get(log_tail))
        .with_state(state)
}

async fn health() -> Json<ApiEnvelope<Value>> {
    ok_json(serde_json::json!({ "status": "ok", "backend": "ferrous-native" }))
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn runtime_info(State(state): State<HostState>) -> Json<ApiEnvelope<HostInfoPayload>> {
    let store = state.manager.store();
    ok_json(HostInfoPayload {
        backend: "ferrous-framework",
        api_compat: "framework-shells-http-mvp",
        transport_owner: "rust-native",
        python_runtime: false,
        socketio: false,
        url: state.base_url,
        store: StorePayload {
            runtime_id: store.runtime_id,
            repo_fingerprint: store.repo_fingerprint,
            root: path_to_string(store.root),
            logs_dir: path_to_string(store.logs_dir),
            metadata_dir: path_to_string(store.metadata_dir),
            sockets_dir: path_to_string(store.sockets_dir),
        },
    })
}

async fn list_shells(
    State(state): State<HostState>,
) -> Result<Json<ApiEnvelope<Vec<ShellPayload>>>, ApiError> {
    let records = state.manager.list_shells().map_err(internal_error)?;
    Ok(ok_json(records.into_iter().map(shell_payload).collect()))
}

async fn get_shell(
    State(state): State<HostState>,
    Path(shell_id): Path<String>,
) -> Result<Json<ApiEnvelope<ShellPayload>>, ApiError> {
    let Some(record) = state.manager.get_shell(&shell_id).map_err(internal_error)? else {
        return Err(not_found("Shell not found"));
    };
    Ok(ok_json(shell_payload(record)))
}

async fn create_shell(
    State(state): State<HostState>,
    headers: HeaderMap,
    Json(request): Json<CreateShellRequest>,
) -> Result<Json<ApiEnvelope<ShellPayload>>, ApiError> {
    require_auth(&state, &headers)?;
    if request.command.is_empty() {
        return Err(bad_request("Command required"));
    }
    let backend = request.backend.as_deref().unwrap_or("pty");
    let label = request.label.clone().unwrap_or_else(|| {
        request
            .spec_id
            .clone()
            .unwrap_or_else(|| "ferrous-shell".to_owned())
    });
    let spec_id = request.spec_id.clone().unwrap_or_else(|| label.clone());
    let log_dir = request.log_dir.map(PathBuf::from);
    let cwd = request.cwd.map(PathBuf::from);
    let mut env = request.env;
    env.entry("TE_FRAMEWORK_URL".to_owned())
        .or_insert_with(|| state.base_url.clone());
    let record = match backend {
        "proc" => state.manager.spawn_proc_blocking(FerrousNativeProcConfig {
            command: request.command,
            cwd,
            env,
            label,
            spec_id,
            subgroups: request.subgroups,
            log_dir,
        }),
        "pipe" => state.manager.spawn_pipe_blocking(FerrousNativePipeConfig {
            command: request.command,
            cwd,
            env,
            label,
            spec_id,
            subgroups: request.subgroups,
            log_dir,
        }),
        "pty" => state.manager.spawn_pty_blocking(FerrousNativePtyConfig {
            command: request.command,
            cwd,
            env,
            label,
            spec_id,
            subgroups: request.subgroups,
            log_dir,
            mode: request.pty_mode.unwrap_or_default(),
        }),
        other => return Err(bad_request(format!("Unsupported backend: {other}"))),
    }
    .map_err(internal_error)?;
    Ok(ok_json(shell_payload(record)))
}

async fn apply_shellspec(
    State(state): State<HostState>,
    headers: HeaderMap,
    Json(request): Json<ShellspecApplyRequest>,
) -> Result<Json<ApiEnvelope<Vec<ShellPayload>>>, ApiError> {
    require_auth(&state, &headers)?;
    let input = ShellspecRenderInput {
        ctx: request.ctx,
        env: request.env,
    };
    let records = state
        .manager
        .apply_shellspec_document_blocking(&request.document, &input, request.prune)
        .map_err(internal_error)?;
    Ok(ok_json(records.into_iter().map(shell_payload).collect()))
}

async fn terminate_shell(
    State(state): State<HostState>,
    Path(shell_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiEnvelope<Value>>, ApiError> {
    require_auth(&state, &headers)?;
    let terminated = state
        .manager
        .terminate_shell_blocking(&shell_id)
        .map_err(internal_error)?;
    if !terminated {
        return Err(not_found("Shell not found or not live"));
    }
    Ok(ok_json(serde_json::json!({ "terminated": true })))
}

async fn shell_action(
    State(state): State<HostState>,
    Path(shell_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ShellActionRequest>,
) -> Result<Json<ApiEnvelope<Value>>, ApiError> {
    require_auth(&state, &headers)?;
    match request.action.as_str() {
        "terminate" => {
            let terminated = state
                .manager
                .terminate_shell_blocking(&shell_id)
                .map_err(internal_error)?;
            if !terminated {
                return Err(not_found("Shell not found or not live"));
            }
            Ok(ok_json(serde_json::json!({ "terminated": true })))
        }
        other => Err(bad_request(format!("Unknown action: {other}"))),
    }
}

async fn shutdown_app_group(
    State(state): State<HostState>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiEnvelope<crate::shutdown::FerrousShutdownResult>>, ApiError> {
    require_auth(&state, &headers)?;
    let result = state
        .manager
        .shutdown_app_group_blocking(&app_id)
        .map_err(internal_error)?;
    Ok(ok_json(result))
}

async fn shell_input(
    State(state): State<HostState>,
    Path(shell_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ShellInputRequest>,
) -> Result<Json<ApiEnvelope<Value>>, ApiError> {
    require_auth(&state, &headers)?;
    if request.eof && request.data.as_deref().unwrap_or_default() != "" {
        return Err(bad_request("Provide either data or eof=true, not both"));
    }
    if request.eof {
        let sent = state
            .manager
            .send_stdin_eof_blocking(&shell_id)
            .map_err(conflict_error)?;
        if !sent {
            return Err(not_found("Shell not found"));
        }
        return Ok(ok_json(serde_json::json!({ "eof": true })));
    }
    let Some(mut data) = request.data else {
        return Err(bad_request("data is required unless eof=true"));
    };
    if request.append_newline {
        data.push('\n');
    }
    let written = state
        .manager
        .write_blocking(&shell_id, data.as_bytes())
        .map_err(conflict_error)?;
    if !written {
        return Err(not_found("Shell not found"));
    }
    Ok(ok_json(serde_json::json!({ "written": data.len() })))
}

async fn log_tail(
    State(state): State<HostState>,
    Path(shell_id): Path<String>,
    Query(query): Query<TailQuery>,
) -> Result<Json<ApiEnvelope<LogTailPayload>>, ApiError> {
    let Some(record) = state.manager.get_shell(&shell_id).map_err(internal_error)? else {
        return Err(not_found("Shell not found"));
    };
    if query.stream != "stdout" && query.stream != "stderr" && query.stream != "both" {
        return Err(bad_request("stream must be stdout, stderr, or both"));
    }
    if record.capabilities.output_read && (query.stream == "stdout" || query.stream == "both") {
        let _ = state
            .manager
            .read_stdout_chunk_blocking(&shell_id, Duration::from_millis(query.drain_timeout_ms));
    }
    let stdout = if query.stream == "stdout" || query.stream == "both" {
        Some(read_log_tail(&record.stdout_log, query.bytes).map_err(internal_error)?)
    } else {
        None
    };
    let stderr = if query.stream == "stderr" || query.stream == "both" {
        Some(read_log_tail(&record.stderr_log, query.bytes).map_err(internal_error)?)
    } else {
        None
    };
    Ok(ok_json(LogTailPayload {
        shell_id,
        stdout,
        stderr,
    }))
}

fn require_auth(state: &HostState, headers: &HeaderMap) -> Result<(), ApiError> {
    if !state.require_auth {
        return Ok(());
    }
    let token = headers
        .get("x-framework-key")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });
    let Some(token) = token else {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "Missing auth token (X-Framework-Key or Authorization header)".to_owned(),
        });
    };
    if !constant_time_eq(token.as_bytes(), state.api_token.as_bytes()) {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "Invalid auth token".to_owned(),
        });
    }
    Ok(())
}

fn read_log_tail(path: &PathBuf, max_bytes: usize) -> Result<LogTailStreamPayload> {
    let max_bytes = max_bytes.clamp(0, 1024 * 1024);
    let mut file =
        File::open(path).with_context(|| format!("failed to open log {}", path.display()))?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(max_bytes as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(LogTailStreamPayload {
        path: path_to_string(path),
        text: String::from_utf8_lossy(&bytes).into_owned(),
        byte_window_start: start,
        byte_window_end: len,
        partial_head: start > 0,
        truncated: start > 0,
    })
}

fn shell_payload(record: FerrousNativeShellRecord) -> ShellPayload {
    ShellPayload {
        id: record.id,
        spec_id: record.spec_id,
        backend: record.backend,
        command: record.command,
        cwd: record.cwd.map(path_to_string),
        pid: record.pid,
        status: record.status,
        exit_code: record.exit_code,
        label: record.label,
        subgroups: record.subgroups,
        record_path: path_to_string(record.record_path),
        stdout_log: path_to_string(record.stdout_log),
        stderr_log: path_to_string(record.stderr_log),
        io_metadata_log: record.io_metadata_log.map(path_to_string),
        pty_mode: record.pty_mode,
        autostart: record.autostart,
        ui: Value::Object(record.ui),
        debug: Value::Object(record.debug),
        runtime_id: record.runtime_id,
        app_id: record.app_id,
        parent_shell_id: record.parent_shell_id,
        is_app_worker: record.is_app_worker,
        capabilities: record.capabilities,
        adopted: record.adopted,
        created_at: record.created_at_ms as f64 / 1000.0,
        updated_at: record.updated_at_ms as f64 / 1000.0,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        env_keys: record.env_keys,
    }
}

fn ok_json<T>(data: T) -> Json<ApiEnvelope<T>> {
    Json(ApiEnvelope { ok: true, data })
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

fn conflict_error(error: anyhow::Error) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        message: error.to_string(),
    }
}

fn internal_error(error: anyhow::Error) -> ApiError {
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: error.to_string(),
    }
}

fn not_found(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        message: message.into(),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorEnvelope {
                ok: false,
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn bind_listener(config: &FerrousNativeHostConfig) -> Result<TcpListener> {
    let mut addrs = (config.host.as_str(), config.port)
        .to_socket_addrs()
        .with_context(|| {
            format!(
                "failed to resolve native host {}:{}",
                config.host, config.port
            )
        })?;
    let addr = addrs
        .next()
        .ok_or_else(|| anyhow!("no resolved native host address"))?;
    TcpListener::bind(addr).with_context(|| format!("failed to bind native host {addr}"))
}

fn url_for_addr(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

fn path_to_string(path: impl AsRef<FsPath>) -> String {
    path.as_ref().to_string_lossy().into_owned()
}

fn default_tail_stream() -> String {
    "both".to_owned()
}

fn default_tail_bytes() -> usize {
    64 * 1024
}

fn default_drain_timeout_ms() -> u64 {
    25
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (left, right) in left.iter().zip(right.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ferrous FWS</title>
  <style>
    body { margin: 0; font: 14px ui-monospace, SFMono-Regular, Menlo, monospace; background: #0d1117; color: #c9d1d9; }
    header { padding: 16px 20px; border-bottom: 1px solid #30363d; background: #161b22; }
    h1 { margin: 0; font-size: 18px; }
    main { padding: 20px; }
    button { color: #c9d1d9; background: #21262d; border: 1px solid #30363d; padding: 6px 10px; border-radius: 6px; }
    pre { white-space: pre-wrap; background: #010409; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
    .muted { color: #8b949e; }
  </style>
</head>
<body>
  <header><h1>Ferrous FWS Native Host</h1><div class="muted">Rust-owned MVP dashboard/control plane</div></header>
  <main>
    <button id="refresh">Refresh Shells</button>
    <pre id="out">Loading...</pre>
  </main>
  <script>
    async function refresh() {
      const out = document.getElementById('out');
      try {
        const [runtime, shells] = await Promise.all([
          fetch('/api/framework_shells/runtime').then((r) => r.json()),
          fetch('/api/framework_shells').then((r) => r.json()),
        ]);
        out.textContent = JSON.stringify({ runtime, shells }, null, 2);
      } catch (error) {
        out.textContent = String(error && error.stack || error);
      }
    }
    document.getElementById('refresh').addEventListener('click', refresh);
    refresh();
  </script>
</body>
</html>"#;
