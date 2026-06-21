use crate::native_runtime::{
    FerrousNativeLifecycleEvent, FerrousNativeLifecycleEventKind, FerrousNativeManager,
    FerrousNativePipeConfig, FerrousNativeProcConfig, FerrousNativePtyConfig, FerrousNativePtyMode,
    FerrousNativeShellRecord, FerrousNativeShellStatus, FerrousNativeStore,
};
use crate::peer_protocol::{
    FWS_BROWSER_ROLE, FWS_DASHBOARD_OPEN_METHOD, FWS_DASHBOARD_REFRESH_METHOD, FWS_DASHBOARD_ROOM,
    FWS_ERROR_METHOD, FWS_LOGS_CLOSE_METHOD, FWS_LOGS_INITIAL_METHOD, FWS_LOGS_OPEN_METHOD,
    FWS_NOTIFICATION_EVENT, FWS_PEER_NOTIFICATION_EVENT, FWS_PEER_REQUEST_EVENT, FWS_PEER_ROLE,
    FWS_PEER_ROOM, FWS_PEER_SUBSCRIPTIONS_EVENT, FWS_SHELL_INPUT_METHOD, FWS_SOCKETIO_NAMESPACE,
    FWS_SOCKETIO_SOCKET_PATH, FwsJsonRpcNotification, FwsPeerShellInputRequest,
    FwsPeerSubscriptions, shell_room,
};
use crate::shellspec::ShellspecRenderInput;
use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::Sha256;
use socketioxide::{
    SocketIo, TransportType,
    extract::{AckSender, Data, SocketRef, TryData},
};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions, remove_file},
    io::{Read, Seek, SeekFrom},
    net::{SocketAddr, TcpListener, ToSocketAddrs},
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
    socketio: Option<SocketIo>,
    socketio_runtime: SocketIoRuntimeState,
}

#[derive(Clone, Default)]
struct SocketIoRuntimeState {
    peer_sids: Arc<Mutex<HashSet<String>>>,
    browser_log_subscriptions: Arc<Mutex<HashMap<String, String>>>,
    peer_notifications_received: Arc<AtomicU64>,
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
    socketio_namespace: &'static str,
    socketio_path: &'static str,
    peer_count: usize,
    peer_notifications_received: u64,
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
    env_overrides: HashMap<String, String>,
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

#[derive(Debug, Default, Deserialize)]
struct ShutdownRequest {
    #[serde(default)]
    root_pids: Vec<i64>,
    #[serde(default)]
    scope: Option<String>,
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
        env.entry("FRAMEWORK_SHELLS_FWS_SOCKETIO_URL".to_owned())
            .or_insert_with(|| self.url());
        env
    }

    pub fn shutdown_tree_blocking(
        &self,
        root_pids: Vec<i64>,
    ) -> Result<crate::shutdown::FerrousShutdownResult> {
        self.manager.shutdown_tree_blocking(root_pids)
    }

    pub fn shutdown_all_blocking(&self) -> Result<crate::shutdown::FerrousShutdownResult> {
        self.manager.shutdown_all_blocking()
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
            socketio: None,
            socketio_runtime: SocketIoRuntimeState::default(),
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
    mut state: HostState,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build native host tokio runtime")?;
    runtime.block_on(async move {
        let (socketio_layer, io) = SocketIo::builder()
            .req_path(FWS_SOCKETIO_SOCKET_PATH)
            .transports([TransportType::Websocket])
            .max_payload(8 * 1024 * 1024)
            .ack_timeout(Duration::from_secs(3))
            .build_layer();
        state.socketio = Some(io.clone());
        register_fws_socketio_namespace(&io, state.clone());
        start_lifecycle_forwarder(io.clone(), state.clone());

        let listener = tokio::net::TcpListener::from_std(listener)
            .context("failed to attach native host listener to tokio")?;
        axum::serve(listener, router(state).layer(socketio_layer))
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
        .route("/fws", get(dashboard_root))
        .route("/fws/", get(dashboard_index))
        .route("/fws/logs/{shell_id}", get(legacy_logs_page))
        .route("/fws/static/{path:path}", get(fws_static))
        .route("/static/vendor/socket.io.min.js", get(socketio_client_js))
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
        .route("/api/framework_shells/shutdown", post(shutdown_tree))
        .route("/api/framework_shells/logs/{shell_id}/tail", get(log_tail))
        .with_state(state)
}

impl SocketIoRuntimeState {
    fn add_peer(&self, sid: String) {
        if let Ok(mut peers) = self.peer_sids.lock() {
            peers.insert(sid);
        }
    }

    fn remove_peer(&self, sid: &str) {
        if let Ok(mut peers) = self.peer_sids.lock() {
            peers.remove(sid);
        }
    }

    fn is_peer(&self, sid: &str) -> bool {
        self.peer_sids
            .lock()
            .map(|peers| peers.contains(sid))
            .unwrap_or(false)
    }

    fn peer_count(&self) -> usize {
        self.peer_sids.lock().map(|peers| peers.len()).unwrap_or(0)
    }

    fn set_browser_log_shell(&self, sid: String, shell_id: Option<String>) -> Option<String> {
        let Ok(mut subscriptions) = self.browser_log_subscriptions.lock() else {
            return None;
        };
        let previous = subscriptions.remove(&sid);
        if let Some(shell_id) = shell_id {
            subscriptions.insert(sid, shell_id);
        }
        previous
    }

    fn remove_browser(&self, sid: &str) -> Option<String> {
        self.browser_log_subscriptions
            .lock()
            .ok()
            .and_then(|mut subscriptions| subscriptions.remove(sid))
    }

    fn active_log_shell_ids(&self) -> Vec<String> {
        let Ok(subscriptions) = self.browser_log_subscriptions.lock() else {
            return Vec::new();
        };
        let mut shell_ids = subscriptions
            .values()
            .filter(|shell_id| !shell_id.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        shell_ids.sort();
        shell_ids.dedup();
        shell_ids
    }
}

fn register_fws_socketio_namespace(io: &SocketIo, state: HostState) {
    let connect_state = state.clone();
    io.ns(
        FWS_SOCKETIO_NAMESPACE,
        move |socket: SocketRef, TryData(auth): TryData<Value>| {
            let state = connect_state.clone();
            async move {
                handle_fws_socketio_connect(socket, auth.ok(), state).await;
            }
        },
    );
}

fn start_lifecycle_forwarder(io: SocketIo, state: HostState) {
    let mut events = state.manager.subscribe_lifecycle();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => emit_shell_lifecycle(&io, event).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

async fn emit_shell_lifecycle(io: &SocketIo, event: FerrousNativeLifecycleEvent) {
    let Some(ns) = io.of(FWS_SOCKETIO_NAMESPACE) else {
        return;
    };
    let method = match event.kind {
        FerrousNativeLifecycleEventKind::Spawned => "fws.shell.spawned",
        FerrousNativeLifecycleEventKind::Updated => "fws.shell.updated",
        FerrousNativeLifecycleEventKind::Exited => "fws.shell.exited",
    };
    let notification = FwsJsonRpcNotification {
        jsonrpc: "2.0".to_owned(),
        method: method.to_owned(),
        params: json!({ "shell": shell_payload(event.shell) }),
    };
    let _ = ns
        .within(FWS_DASHBOARD_ROOM)
        .emit(FWS_NOTIFICATION_EVENT, &notification)
        .await;
}

async fn handle_fws_socketio_connect(socket: SocketRef, auth: Option<Value>, state: HostState) {
    let sid = socket.id.to_string();
    let role = auth
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|mapping| mapping.get("role"))
        .and_then(Value::as_str)
        .unwrap_or(FWS_BROWSER_ROLE);

    if role == FWS_PEER_ROLE {
        if !peer_auth_valid(&state, auth.as_ref()) {
            let _ = socket.disconnect();
            return;
        }
        state.socketio_runtime.add_peer(sid.clone());
        socket.join(FWS_PEER_ROOM);
        let _ = socket.emit(
            FWS_PEER_SUBSCRIPTIONS_EVENT,
            &FwsPeerSubscriptions {
                shell_ids: state.socketio_runtime.active_log_shell_ids(),
            },
        );
    }

    let disconnect_state = state.clone();
    socket.on_disconnect(move |socket: SocketRef| {
        let state = disconnect_state.clone();
        async move {
            let sid = socket.id.to_string();
            state.socketio_runtime.remove_peer(&sid);
            let previous_shell_id = state.socketio_runtime.remove_browser(&sid);
            if let Some(shell_id) = previous_shell_id {
                broadcast_peer_subscriptions(&state).await;
                socket.leave(shell_room(&shell_id));
            }
        }
    });

    let notification_state = state.clone();
    socket.on(
        FWS_PEER_NOTIFICATION_EVENT,
        move |socket: SocketRef, Data::<Value>(notification)| {
            let state = notification_state.clone();
            async move {
                handle_peer_notification(socket, notification, state).await;
            }
        },
    );

    let request_state = state;
    socket.on(
        crate::peer_protocol::FWS_REQUEST_EVENT,
        move |socket: SocketRef, Data::<Value>(request), ack: AckSender| {
            let state = request_state.clone();
            async move {
                let response = handle_browser_request(socket, request, state).await;
                let _ = ack.send(&response);
            }
        },
    );
}

fn peer_auth_valid(state: &HostState, auth: Option<&Value>) -> bool {
    let Some(mapping) = auth.and_then(Value::as_object) else {
        return false;
    };
    let api_token = mapping.get("api_token").and_then(Value::as_str);
    let runtime_id = mapping.get("runtime_id").and_then(Value::as_str);
    api_token == Some(state.api_token.as_str())
        && runtime_id == Some(state.manager.store().runtime_id.as_str())
}

async fn handle_peer_notification(socket: SocketRef, notification: Value, state: HostState) {
    let sid = socket.id.to_string();
    if !state.socketio_runtime.is_peer(&sid) {
        return;
    }
    let Some(method) = notification.get("method").and_then(Value::as_str) else {
        return;
    };
    if notification.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return;
    }
    state
        .socketio_runtime
        .peer_notifications_received
        .fetch_add(1, Ordering::Relaxed);

    if matches!(
        method,
        "fws.shell.created"
            | "fws.shell.spawned"
            | "fws.shell.updated"
            | "fws.shell.exited"
            | "fws.shell.removed"
    ) {
        let _ = socket
            .to(FWS_DASHBOARD_ROOM)
            .emit(FWS_NOTIFICATION_EVENT, &notification)
            .await;
        return;
    }

    if matches!(
        method,
        FWS_LOGS_INITIAL_METHOD
            | crate::peer_protocol::FWS_LOGS_CHUNK_METHOD
            | crate::peer_protocol::FWS_LOGS_IO_METADATA_METHOD
            | crate::peer_protocol::FWS_LOGS_RESET_METHOD
            | FWS_ERROR_METHOD
    ) {
        if let Some(shell_id) = notification
            .get("params")
            .and_then(Value::as_object)
            .and_then(|params| params.get("shell_id"))
            .and_then(Value::as_str)
        {
            let _ = socket
                .to(shell_room(shell_id))
                .emit(FWS_NOTIFICATION_EVENT, &notification)
                .await;
        }
    }
}

async fn handle_browser_request(socket: SocketRef, request: Value, state: HostState) -> Value {
    let request_id = request.get("id").and_then(Value::as_str).map(str::to_owned);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let params = request.get("params").and_then(Value::as_object);

    match method {
        FWS_DASHBOARD_OPEN_METHOD => {
            socket.join(FWS_DASHBOARD_ROOM);
            dashboard_state_response(request_id, true, &state)
        }
        FWS_DASHBOARD_REFRESH_METHOD => dashboard_state_response(request_id, false, &state),
        FWS_LOGS_OPEN_METHOD => {
            let Some(shell_id) = params
                .and_then(|params| params.get("shell_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
            else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "shell_id is required",
                    "invalid_params",
                    None,
                );
            };
            if let Err(error) = emit_logs_initial(&socket, &state, &shell_id).await {
                return jsonrpc_error(
                    request_id,
                    -32004,
                    &error.to_string(),
                    "not_found",
                    Some(&shell_id),
                );
            }
            set_browser_log_shell(&socket, &state, Some(shell_id.clone())).await;
            json!({"jsonrpc": "2.0", "id": request_id, "result": {"accepted": true, "shell_id": shell_id}})
        }
        FWS_LOGS_CLOSE_METHOD => {
            let shell_id = params
                .and_then(|params| params.get("shell_id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(shell_id) = shell_id {
                let current = state
                    .socketio_runtime
                    .browser_log_subscriptions
                    .lock()
                    .ok()
                    .and_then(|subscriptions| subscriptions.get(&socket.id.to_string()).cloned());
                if current.as_deref() == Some(shell_id.as_str()) {
                    set_browser_log_shell(&socket, &state, None).await;
                }
            }
            json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}})
        }
        "fws.logs.truncate" => match truncate_all_logs(&state) {
            Ok(()) => json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}}),
            Err(error) => jsonrpc_error(
                request_id,
                -32000,
                &error.to_string(),
                "action_failed",
                None,
            ),
        },
        "fws.exited.purge" => match purge_exited_records(&state) {
            Ok(removed_shell_ids) => {
                for shell_id in &removed_shell_ids {
                    emit_shell_removed(&state, shell_id).await;
                }
                json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}})
            }
            Err(error) => jsonrpc_error(
                request_id,
                -32000,
                &error.to_string(),
                "action_failed",
                None,
            ),
        },
        "fws.shell.terminate" => {
            let Some(shell_id) = params
                .and_then(|params| params.get("shell_id"))
                .and_then(Value::as_str)
            else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "shell_id is required",
                    "invalid_params",
                    None,
                );
            };
            match state.manager.terminate_shell_blocking(shell_id) {
                Ok(true) => json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}}),
                Ok(false) => jsonrpc_error(
                    request_id,
                    -32004,
                    "Shell not found",
                    "not_found",
                    Some(shell_id),
                ),
                Err(error) => jsonrpc_error(
                    request_id,
                    -32000,
                    &error.to_string(),
                    "action_failed",
                    Some(shell_id),
                ),
            }
        }
        "fws.shell.purge" => {
            let Some(shell_id) = params
                .and_then(|params| params.get("shell_id"))
                .and_then(Value::as_str)
            else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "shell_id is required",
                    "invalid_params",
                    None,
                );
            };
            match purge_shell_record(&state, shell_id) {
                Ok(()) => {
                    emit_shell_removed(&state, shell_id).await;
                    json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}})
                }
                Err(error) => jsonrpc_error(
                    request_id,
                    -32000,
                    &error.to_string(),
                    "action_failed",
                    Some(shell_id),
                ),
            }
        }
        "fws.pid.terminate" => {
            let Some(pid) = params
                .and_then(|params| params.get("pid"))
                .and_then(Value::as_i64)
            else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "pid is required",
                    "invalid_params",
                    None,
                );
            };
            match terminate_pid(pid) {
                Ok(()) => json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}}),
                Err(error) => jsonrpc_error(
                    request_id,
                    -32000,
                    &error.to_string(),
                    "action_failed",
                    None,
                ),
            }
        }
        "fws.app.shutdown" => {
            let Some(app_id) = params
                .and_then(|params| params.get("app_id"))
                .and_then(Value::as_str)
            else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "app_id is required",
                    "invalid_params",
                    None,
                );
            };
            match state.manager.shutdown_app_group_blocking(app_id) {
                Ok(shutdown) => {
                    json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true, "shutdown": shutdown}})
                }
                Err(error) => jsonrpc_error(
                    request_id,
                    -32000,
                    &error.to_string(),
                    "action_failed",
                    None,
                ),
            }
        }
        "fws.shutdown" => {
            let scope = params
                .and_then(|params| params.get("scope"))
                .and_then(Value::as_str)
                .unwrap_or("tree");
            let result = match scope {
                "shells" | "all" => state.manager.shutdown_all_blocking(),
                "tree" => state.manager.shutdown_tree_blocking(Vec::new()),
                _ => Err(anyhow!("Unknown shutdown scope: {scope}")),
            };
            match result {
                Ok(shutdown) => {
                    json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true, "shutdown": shutdown}})
                }
                Err(error) => jsonrpc_error(
                    request_id,
                    -32000,
                    &error.to_string(),
                    "action_failed",
                    None,
                ),
            }
        }
        FWS_SHELL_INPUT_METHOD => {
            let Some(params) = params else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "params are required",
                    "invalid_params",
                    None,
                );
            };
            let Some(shell_id) = params.get("shell_id").and_then(Value::as_str) else {
                return jsonrpc_error(
                    request_id,
                    -32602,
                    "shell_id is required",
                    "invalid_params",
                    None,
                );
            };
            let data = params.get("data").and_then(Value::as_str);
            match write_shell_input_control(
                &state,
                shell_id,
                data,
                params
                    .get("append_newline")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                params.get("eof").and_then(Value::as_bool).unwrap_or(false),
                "dashboard",
            )
            .await
            {
                Ok(_) => json!({"jsonrpc": "2.0", "id": request_id, "result": {"ok": true}}),
                Err(error) => jsonrpc_error(
                    request_id,
                    error.status.as_u16() as i64,
                    &error.message,
                    "action_failed",
                    Some(shell_id),
                ),
            }
        }
        _ => jsonrpc_error(
            request_id,
            -32601,
            &format!("Method not found: {method}"),
            "method_not_found",
            None,
        ),
    }
}

fn dashboard_state_response(
    request_id: Option<String>,
    accepted: bool,
    state: &HostState,
) -> Value {
    let shells = state
        .manager
        .list_shells()
        .map(|records| {
            records
                .into_iter()
                .map(shell_payload)
                .map(|payload| serde_json::to_value(payload).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let result = if accepted {
        json!({"accepted": true, "state": {"shells": shells, "processes": []}})
    } else {
        json!({"ok": true, "state": {"shells": shells, "processes": []}})
    };
    json!({"jsonrpc": "2.0", "id": request_id, "result": result})
}

async fn emit_logs_initial(socket: &SocketRef, state: &HostState, shell_id: &str) -> Result<()> {
    let Some(record) = state.manager.get_shell(shell_id)? else {
        return Err(anyhow!("Shell not found: {shell_id}"));
    };
    let stdout = read_log_text_lossy(&record.stdout_log)?;
    let stderr = read_log_text_lossy(&record.stderr_log)?;
    let notification = FwsJsonRpcNotification {
        jsonrpc: "2.0".to_owned(),
        method: FWS_LOGS_INITIAL_METHOD.to_owned(),
        params: json!({
            "shell_id": shell_id,
            "stdout": stdout,
            "stderr": stderr,
            "io_metadata": []
        }),
    };
    let _ = socket.emit(FWS_NOTIFICATION_EVENT, &notification);
    Ok(())
}

async fn set_browser_log_shell(socket: &SocketRef, state: &HostState, shell_id: Option<String>) {
    let sid = socket.id.to_string();
    if let Some(previous_shell_id) = state
        .socketio_runtime
        .set_browser_log_shell(sid, shell_id.clone())
    {
        socket.leave(shell_room(&previous_shell_id));
    }
    if let Some(shell_id) = shell_id {
        socket.join(shell_room(&shell_id));
    }
    broadcast_peer_subscriptions(state).await;
}

async fn write_shell_input_control(
    state: &HostState,
    shell_id: &str,
    data: Option<&str>,
    append_newline: bool,
    eof: bool,
    source: &str,
) -> Result<Value, ApiError> {
    if eof {
        match state.manager.send_stdin_eof_blocking(shell_id) {
            Ok(true) => return Ok(json!({"eof": true})),
            Ok(false) => {}
            Err(error) if local_input_error_can_fallback(&error) => {}
            Err(error) => return Err(conflict_error(error)),
        }
    } else {
        let Some(data) = data else {
            return Err(bad_request("data is required unless eof=true"));
        };
        let mut bytes = data.as_bytes().to_vec();
        if append_newline {
            bytes.push(b'\n');
        }
        match state.manager.write_blocking(shell_id, &bytes) {
            Ok(true) => {
                return Ok(json!({
                    "written": bytes.len(),
                    "shell_id": shell_id,
                    "accepted": true,
                    "newline_appended": append_newline,
                    "eof_sent": false
                }));
            }
            Ok(false) => {}
            Err(error) if local_input_error_can_fallback(&error) => {}
            Err(error) => return Err(conflict_error(error)),
        }
    }

    call_peer_shell_input(state, shell_id, data, append_newline, eof, source).await
}

async fn call_peer_shell_input(
    state: &HostState,
    shell_id: &str,
    data: Option<&str>,
    append_newline: bool,
    eof: bool,
    source: &str,
) -> Result<Value, ApiError> {
    if state.socketio_runtime.peer_count() == 0 {
        return Err(not_found(format!(
            "Live input unavailable for shell {shell_id}: no connected FWS peer owns live input"
        )));
    }
    let Some(io) = &state.socketio else {
        return Err(not_found(format!(
            "Live input unavailable for shell {shell_id}: Socket.IO controller is unavailable"
        )));
    };
    let Some(ns) = io.of(FWS_SOCKETIO_NAMESPACE) else {
        return Err(not_found(format!(
            "Live input unavailable for shell {shell_id}: namespace {FWS_SOCKETIO_NAMESPACE} is unavailable"
        )));
    };
    let request = FwsPeerShellInputRequest::new(
        shell_id.to_owned(),
        data.unwrap_or_default().to_owned(),
        append_newline,
        eof,
        source.to_owned(),
    );
    let ack_stream = ns
        .within(FWS_PEER_ROOM)
        .timeout(Duration::from_secs(3))
        .emit_with_ack::<_, Value>(FWS_PEER_REQUEST_EVENT, &request)
        .await
        .map_err(|error| conflict_error(anyhow!("peer request send failed: {error}")))?;
    futures_util::pin_mut!(ack_stream);

    let mut fallback_errors = Vec::new();
    while let Some((_sid, ack)) = ack_stream.next().await {
        let value = match ack {
            Ok(value) => value,
            Err(error) => {
                fallback_errors.push(error.to_string());
                continue;
            }
        };
        match parse_peer_response_value(value) {
            PeerResponseValue::Accepted(data) => return Ok(data),
            PeerResponseValue::Fallback => {}
            PeerResponseValue::Failed(error) => fallback_errors.push(error),
        }
    }

    if let Some(error) = fallback_errors.into_iter().next() {
        return Err(conflict_error(anyhow!(error)));
    }
    Err(not_found(format!(
        "Live input unavailable for shell {shell_id}: no connected FWS peer accepted the write"
    )))
}

enum PeerResponseValue {
    Accepted(Value),
    Fallback,
    Failed(String),
}

fn parse_peer_response_value(value: Value) -> PeerResponseValue {
    let Some(mapping) = value.as_object() else {
        return PeerResponseValue::Fallback;
    };
    if mapping.get("ok").and_then(Value::as_bool) == Some(true) {
        return PeerResponseValue::Accepted(
            mapping
                .get("data")
                .cloned()
                .unwrap_or_else(|| json!({"ok": true})),
        );
    }
    let code = mapping.get("code").and_then(Value::as_str).unwrap_or("");
    if matches!(code, "not_owner" | "not_found") {
        return PeerResponseValue::Fallback;
    }
    let error = mapping
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("peer request failed");
    PeerResponseValue::Failed(error.to_owned())
}

fn local_input_error_can_fallback(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("not found")
        || message.contains("not live")
        || message.contains("does not expose")
        || message.contains("unavailable")
}

async fn broadcast_peer_subscriptions(state: &HostState) {
    let Some(io) = &state.socketio else {
        return;
    };
    let Some(ns) = io.of(FWS_SOCKETIO_NAMESPACE) else {
        return;
    };
    let payload = FwsPeerSubscriptions {
        shell_ids: state.socketio_runtime.active_log_shell_ids(),
    };
    let _ = ns
        .within(FWS_PEER_ROOM)
        .emit(FWS_PEER_SUBSCRIPTIONS_EVENT, &payload)
        .await;
}

fn truncate_all_logs(state: &HostState) -> Result<()> {
    for record in state.manager.list_shells()? {
        truncate_log_file(&record.stdout_log)?;
        truncate_log_file(&record.stderr_log)?;
    }
    Ok(())
}

fn truncate_log_file(path: &PathBuf) -> Result<()> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to truncate log {}", path.display()))?;
    Ok(())
}

fn purge_exited_records(state: &HostState) -> Result<Vec<String>> {
    let mut removed_shell_ids = Vec::new();
    for record in state.manager.list_persisted_records()? {
        if record.status == FerrousNativeShellStatus::Exited {
            purge_record_files(&record)?;
            removed_shell_ids.push(record.id);
        }
    }
    Ok(removed_shell_ids)
}

fn purge_shell_record(state: &HostState, shell_id: &str) -> Result<()> {
    let Some(record) = state.manager.get_shell(shell_id)? else {
        return Err(anyhow!("Shell not found: {shell_id}"));
    };
    if record.status == FerrousNativeShellStatus::Running {
        return Err(anyhow!("Refusing to purge running shell: {shell_id}"));
    }
    purge_record_files(&record)
}

fn purge_record_files(record: &FerrousNativeShellRecord) -> Result<()> {
    remove_file_if_exists(&record.stdout_log)?;
    remove_file_if_exists(&record.stderr_log)?;
    if let Some(path) = &record.io_metadata_log {
        remove_file_if_exists(path)?;
    }
    remove_file_if_exists(&record.record_path)?;
    Ok(())
}

async fn emit_shell_removed(state: &HostState, shell_id: &str) {
    let Some(io) = &state.socketio else {
        return;
    };
    let Some(ns) = io.of(FWS_SOCKETIO_NAMESPACE) else {
        return;
    };
    let notification = FwsJsonRpcNotification {
        jsonrpc: "2.0".to_owned(),
        method: "fws.shell.removed".to_owned(),
        params: json!({ "shell_id": shell_id }),
    };
    let _ = ns
        .within(FWS_DASHBOARD_ROOM)
        .emit(FWS_NOTIFICATION_EVENT, &notification)
        .await;
}

fn remove_file_if_exists(path: &PathBuf) -> Result<()> {
    match remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn terminate_pid(pid: i64) -> Result<()> {
    if pid <= 0 || pid > i64::from(i32::MAX) {
        return Err(anyhow!("invalid pid: {pid}"));
    }
    let rc = unsafe { nix::libc::kill(pid as nix::libc::pid_t, nix::libc::SIGTERM) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to terminate pid {pid}"))
    }
}

fn read_log_text_lossy(path: &PathBuf) -> Result<String> {
    if !path.exists() {
        return Ok(String::new());
    }
    let mut file =
        File::open(path).with_context(|| format!("failed to open log {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn jsonrpc_error(
    request_id: Option<String>,
    code: i64,
    message: &str,
    error_code: &str,
    shell_id: Option<&str>,
) -> Value {
    let mut data = Map::new();
    data.insert(
        "error_code".to_owned(),
        Value::String(error_code.to_owned()),
    );
    if let Some(shell_id) = shell_id {
        data.insert("shell_id".to_owned(), Value::String(shell_id.to_owned()));
    }
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": code,
            "message": message,
            "data": Value::Object(data),
        }
    })
}

async fn health() -> Json<ApiEnvelope<Value>> {
    ok_json(serde_json::json!({ "status": "ok", "backend": "ferrous-native" }))
}

async fn dashboard_root() -> Redirect {
    Redirect::permanent("/fws/")
}

async fn dashboard_index() -> Response {
    no_store_response("text/html; charset=utf-8", FWS_INDEX_HTML)
}

async fn legacy_logs_page(Path(shell_id): Path<String>) -> Response {
    no_store_response(
        "text/html; charset=utf-8",
        FWS_LOGS_HTML.replace("{{ shell_id }}", &shell_id),
    )
}

async fn fws_static(Path(path): Path<String>) -> Result<Response, ApiError> {
    match path.as_str() {
        "fws.css" => Ok(no_store_response("text/css; charset=utf-8", FWS_CSS)),
        "fws.js" => Ok(no_store_response(
            "application/javascript; charset=utf-8",
            FWS_JS,
        )),
        "index.html" => Ok(no_store_response(
            "text/html; charset=utf-8",
            FWS_INDEX_HTML,
        )),
        "logs.html" => Ok(no_store_response("text/html; charset=utf-8", FWS_LOGS_HTML)),
        _ => Err(not_found("Not found")),
    }
}

async fn socketio_client_js() -> Response {
    no_store_bytes_response("application/javascript; charset=utf-8", SOCKET_IO_CLIENT_JS)
}

async fn runtime_info(State(state): State<HostState>) -> Json<ApiEnvelope<HostInfoPayload>> {
    let store = state.manager.store();
    ok_json(HostInfoPayload {
        backend: "ferrous-framework",
        api_compat: "framework-shells-http-socketio-mvp",
        transport_owner: "rust-native",
        python_runtime: false,
        socketio: state.socketio.is_some(),
        socketio_namespace: FWS_SOCKETIO_NAMESPACE,
        socketio_path: FWS_SOCKETIO_SOCKET_PATH,
        peer_count: state.socketio_runtime.peer_count(),
        peer_notifications_received: state
            .socketio_runtime
            .peer_notifications_received
            .load(Ordering::Relaxed),
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

async fn shutdown_tree(
    State(state): State<HostState>,
    headers: HeaderMap,
    body: Option<Json<ShutdownRequest>>,
) -> Result<Json<ApiEnvelope<crate::shutdown::FerrousShutdownResult>>, ApiError> {
    require_auth(&state, &headers)?;
    let request = body.map(|Json(request)| request).unwrap_or_default();
    let result = match request.scope.as_deref() {
        Some("all") => state.manager.shutdown_all_blocking(),
        Some("tree") | None => state.manager.shutdown_tree_blocking(request.root_pids),
        Some(other) => return Err(bad_request(format!("Unknown shutdown scope: {other}"))),
    }
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
    if !request.eof && request.data.is_none() {
        return Err(bad_request("data is required unless eof=true"));
    }
    let result = write_shell_input_control(
        &state,
        &shell_id,
        request.data.as_deref(),
        request.append_newline,
        request.eof,
        "http",
    )
    .await?;
    Ok(ok_json(result))
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
        let _ = state.manager.flush_stdout_log_blocking(&shell_id);
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
        env_overrides: record.env_overrides,
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

fn no_store_response(content_type: &'static str, body: impl Into<Body>) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CACHE_CONTROL,
            "no-store, no-cache, must-revalidate, max-age=0",
        )
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(body.into())
        .expect("static response builder is valid")
}

fn no_store_bytes_response(content_type: &'static str, body: &'static [u8]) -> Response {
    no_store_response(content_type, Body::from(body.to_vec()))
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

const FWS_INDEX_HTML: &str = include_str!("../assets/fws_ui/index.html");
const FWS_CSS: &str = include_str!("../assets/fws_ui/fws.css");
const FWS_JS: &str = include_str!("../assets/fws_ui/fws.js");
const FWS_LOGS_HTML: &str = include_str!("../assets/fws_ui/logs.html");
const SOCKET_IO_CLIENT_JS: &[u8] = include_bytes!("../assets/vendor/socket.io.min.js");
