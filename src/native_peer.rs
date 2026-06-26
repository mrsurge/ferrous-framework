use crate::native_host::derive_api_token;
use crate::native_runtime::{
    FerrousNativeLifecycleEvent, FerrousNativeLifecycleEventKind, FerrousNativeManager,
    FerrousNativeOutputChunk, FerrousNativeOutputStream, FerrousNativeOutputSubscription,
    FerrousNativeOutputSubscriptionStopper, FerrousNativeShellRecord, FerrousShellInputResult,
};
use crate::peer_protocol::{
    FWS_LOGS_CHUNK_METHOD, FWS_PEER_NOTIFICATION_EVENT, FWS_PEER_REQUEST_EVENT,
    FWS_PEER_SUBSCRIPTIONS_EVENT, FWS_SHELL_INPUT_METHOD, FWS_SOCKETIO_NAMESPACE,
    FWS_SOCKETIO_SOCKET_PATH, FwsJsonRpcNotification, FwsPeerAuth, FwsPeerErrorResponse,
    FwsPeerResponse, FwsPeerShellInputRequest, FwsPeerSubscriptions, FwsPeerSuccessResponse,
    notification_shell_id, peer_notification_requires_subscription,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
};
use tf_rust_socketio::{ClientBuilder, Payload, RawClient, TransportType, client::Client};
use tokio::sync::{broadcast::error::RecvError, watch};

const PEER_OUTPUT_SUBSCRIPTION_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativePeerConfig {
    pub controller_url: String,
    pub reconnect: bool,
    pub reconnect_on_disconnect: bool,
    pub max_reconnect_attempts: Option<u8>,
}

impl FerrousNativePeerConfig {
    pub fn new(controller_url: impl Into<String>) -> Self {
        Self {
            controller_url: controller_url.into(),
            reconnect: true,
            reconnect_on_disconnect: true,
            max_reconnect_attempts: None,
        }
    }
}

pub struct FerrousNativePeer {
    client: Client,
    subscriptions: Arc<Mutex<HashSet<String>>>,
    relay_shutdown: Arc<AtomicBool>,
    relay_shutdown_tx: watch::Sender<bool>,
    subscription_wake: Sender<()>,
    relay_threads: Vec<JoinHandle<()>>,
}

impl FerrousNativePeer {
    pub fn connect(manager: FerrousNativeManager, config: FerrousNativePeerConfig) -> Result<Self> {
        let subscriptions = Arc::new(Mutex::new(HashSet::new()));
        let subscription_state = Arc::clone(&subscriptions);
        let (subscription_tx, subscription_rx) = mpsc::channel::<()>();
        let subscription_wake_for_event = subscription_tx.clone();
        let request_manager = manager.clone();
        let auth = FwsPeerAuth::new(
            derive_api_token(&manager.native_env().secret),
            manager.store().runtime_id,
            process::id().to_string(),
        );
        let mut builder = ClientBuilder::new(socketio_url(&config.controller_url))
            .namespace(FWS_SOCKETIO_NAMESPACE)
            .transport_type(TransportType::Websocket)
            .reconnect(config.reconnect)
            .reconnect_on_disconnect(config.reconnect_on_disconnect)
            .auth(serde_json::to_value(auth)?)
            .on(FWS_PEER_SUBSCRIPTIONS_EVENT, move |payload, _socket| {
                update_subscriptions(&subscription_state, payload);
                let _ = subscription_wake_for_event.send(());
            })
            .on(FWS_PEER_REQUEST_EVENT, move |payload, socket| {
                acknowledge_peer_request(&request_manager, payload, socket);
            });
        if let Some(max_reconnect_attempts) = config.max_reconnect_attempts {
            builder = builder.max_reconnect_attempts(max_reconnect_attempts);
        }
        let client = builder
            .connect()
            .with_context(|| format!("failed to connect FWS peer to {}", config.controller_url))?;
        let relay_shutdown = Arc::new(AtomicBool::new(false));
        let (relay_shutdown_tx, relay_shutdown_rx) = watch::channel(false);
        let relay_threads = start_relay_workers(
            manager,
            client.clone(),
            Arc::clone(&subscriptions),
            Arc::clone(&relay_shutdown),
            relay_shutdown_rx,
            subscription_rx,
        );
        let _ = subscription_tx.send(());
        Ok(Self {
            client,
            subscriptions,
            relay_shutdown,
            relay_shutdown_tx,
            subscription_wake: subscription_tx,
            relay_threads,
        })
    }

    pub fn connect_from_manager_env(manager: FerrousNativeManager) -> Result<Self> {
        let env = manager.native_env();
        let Some(controller_url) = env.fws_socketio_url.or(env.te_framework_url) else {
            return Err(anyhow!(
                "Ferrous peer requires FRAMEWORK_SHELLS_FWS_SOCKETIO_URL or TE_FRAMEWORK_URL"
            ));
        };
        Self::connect(manager, FerrousNativePeerConfig::new(controller_url))
    }

    pub fn subscriptions(&self) -> Vec<String> {
        let Ok(subscriptions) = self.subscriptions.lock() else {
            return Vec::new();
        };
        let mut shell_ids = subscriptions.iter().cloned().collect::<Vec<_>>();
        shell_ids.sort();
        shell_ids
    }

    pub fn emit_notification(&self, notification: FwsJsonRpcNotification) -> Result<bool> {
        if peer_notification_requires_subscription(&notification.method) {
            let Some(shell_id) = notification_shell_id(&notification) else {
                return Ok(false);
            };
            let subscribed = self
                .subscriptions
                .lock()
                .map(|subscriptions| subscriptions.contains(&shell_id))
                .unwrap_or(false);
            if !subscribed {
                return Ok(false);
            }
        }
        emit_peer_notification(&self.client, notification)?;
        Ok(true)
    }

    pub fn disconnect(&self) -> Result<()> {
        self.relay_shutdown.store(true, Ordering::Relaxed);
        let _ = self.relay_shutdown_tx.send(true);
        let _ = self.subscription_wake.send(());
        self.client
            .disconnect()
            .map_err(|error| anyhow!("failed to disconnect FWS peer: {error}"))
    }
}

impl Drop for FerrousNativePeer {
    fn drop(&mut self) {
        self.relay_shutdown.store(true, Ordering::Relaxed);
        let _ = self.relay_shutdown_tx.send(true);
        let _ = self.subscription_wake.send(());
        let _ = self.client.disconnect();
        for thread in self.relay_threads.drain(..) {
            let _ = thread.join();
        }
    }
}

fn start_relay_workers(
    manager: FerrousNativeManager,
    client: Client,
    subscriptions: Arc<Mutex<HashSet<String>>>,
    shutdown: Arc<AtomicBool>,
    lifecycle_shutdown: watch::Receiver<bool>,
    subscription_rx: Receiver<()>,
) -> Vec<JoinHandle<()>> {
    let mut threads = start_lifecycle_relay(manager.clone(), client.clone(), lifecycle_shutdown);
    threads.push(start_output_relay(
        manager,
        client,
        subscriptions,
        shutdown,
        subscription_rx,
    ));
    threads
}

fn start_lifecycle_relay(
    manager: FerrousNativeManager,
    client: Client,
    mut shutdown: watch::Receiver<bool>,
) -> Vec<JoinHandle<()>> {
    let (notification_tx, notification_rx) = mpsc::channel::<FwsJsonRpcNotification>();
    let event_thread = thread::spawn(move || {
        let mut events = manager.subscribe_lifecycle();
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        runtime.block_on(async move {
            loop {
                if *shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    event = events.recv() => match event {
                        Ok(event) => {
                            let _ = notification_tx.send(lifecycle_notification(event));
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => break,
                    }
                }
            }
        });
    });
    let emit_thread = thread::spawn(move || {
        while let Ok(notification) = notification_rx.recv() {
            let _ = emit_peer_notification(&client, notification);
        }
    });
    vec![event_thread, emit_thread]
}

fn start_output_relay(
    manager: FerrousNativeManager,
    client: Client,
    subscriptions: Arc<Mutex<HashSet<String>>>,
    shutdown: Arc<AtomicBool>,
    subscription_rx: Receiver<()>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let active_streams = Arc::new(Mutex::new(HashMap::<
            (String, FerrousNativeOutputStream),
            FerrousNativeOutputSubscriptionStopper,
        >::new()));
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            if subscription_rx.recv().is_err() {
                break;
            }
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            ensure_peer_output_streams(
                &manager,
                &client,
                &subscriptions,
                Arc::clone(&active_streams),
                Arc::clone(&shutdown),
            );
        }
        if let Ok(mut active) = active_streams.lock() {
            for (_, stopper) in active.drain() {
                stopper.stop();
            }
        }
    })
}

fn ensure_peer_output_streams(
    manager: &FerrousNativeManager,
    client: &Client,
    subscriptions: &Arc<Mutex<HashSet<String>>>,
    active_streams: Arc<
        Mutex<HashMap<(String, FerrousNativeOutputStream), FerrousNativeOutputSubscriptionStopper>>,
    >,
    shutdown: Arc<AtomicBool>,
) {
    let desired = desired_output_streams(subscriptions);
    if let Ok(mut active) = active_streams.lock() {
        let stale = active
            .keys()
            .filter(|key| !desired.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in stale {
            if let Some(stopper) = active.remove(&key) {
                stopper.stop();
            }
        }
    }
    for (shell_id, stream) in desired {
        let key = (shell_id.clone(), stream);
        let already_active = active_streams
            .lock()
            .map(|active| active.contains_key(&key))
            .unwrap_or(true);
        if already_active {
            continue;
        }
        let Ok(Some(subscription)) =
            manager.subscribe_output(&shell_id, stream, PEER_OUTPUT_SUBSCRIPTION_CAPACITY)
        else {
            continue;
        };
        let stopper = subscription.stopper();
        if let Ok(mut active) = active_streams.lock() {
            active.insert(key.clone(), stopper);
        }
        spawn_output_stream_relay(
            client.clone(),
            subscription,
            key,
            Arc::clone(&active_streams),
            Arc::clone(&shutdown),
        );
    }
}

fn desired_output_streams(
    subscriptions: &Arc<Mutex<HashSet<String>>>,
) -> HashSet<(String, FerrousNativeOutputStream)> {
    let Ok(subscriptions) = subscriptions.lock() else {
        return HashSet::new();
    };
    subscriptions
        .iter()
        .flat_map(|shell_id| {
            [
                (shell_id.clone(), FerrousNativeOutputStream::Stdout),
                (shell_id.clone(), FerrousNativeOutputStream::Stderr),
            ]
        })
        .collect()
}

fn spawn_output_stream_relay(
    client: Client,
    subscription: FerrousNativeOutputSubscription,
    key: (String, FerrousNativeOutputStream),
    active_streams: Arc<
        Mutex<HashMap<(String, FerrousNativeOutputStream), FerrousNativeOutputSubscriptionStopper>>,
    >,
    shutdown: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            let active = active_streams
                .lock()
                .map(|active| active.contains_key(&key))
                .unwrap_or(false);
            if !active {
                break;
            }
            match subscription.recv() {
                Ok(Some(chunk)) => {
                    let _ = emit_peer_notification(&client, output_chunk_notification(chunk));
                }
                Ok(None) | Err(_) => break,
            }
        }
        if let Ok(mut active) = active_streams.lock()
            && let Some(stopper) = active.remove(&key)
        {
            stopper.stop();
        }
    });
}

fn lifecycle_notification(event: FerrousNativeLifecycleEvent) -> FwsJsonRpcNotification {
    let method = match event.kind {
        FerrousNativeLifecycleEventKind::Spawned => "fws.shell.spawned",
        FerrousNativeLifecycleEventKind::Updated => "fws.shell.updated",
        FerrousNativeLifecycleEventKind::Exited => "fws.shell.exited",
    };
    FwsJsonRpcNotification {
        jsonrpc: "2.0".to_owned(),
        method: method.to_owned(),
        params: json!({ "shell": shell_payload_value(event.shell) }),
    }
}

fn output_chunk_notification(chunk: FerrousNativeOutputChunk) -> FwsJsonRpcNotification {
    FwsJsonRpcNotification {
        jsonrpc: "2.0".to_owned(),
        method: FWS_LOGS_CHUNK_METHOD.to_owned(),
        params: json!({
            "shell_id": chunk.shell_id,
            "stream": output_stream_name(chunk.stream),
            "chunk": String::from_utf8_lossy(&chunk.bytes).into_owned(),
            "dropped_before": chunk.dropped_before,
        }),
    }
}

fn output_stream_name(stream: FerrousNativeOutputStream) -> &'static str {
    match stream {
        FerrousNativeOutputStream::Stdout => "stdout",
        FerrousNativeOutputStream::Stderr => "stderr",
    }
}

fn emit_peer_notification(client: &Client, notification: FwsJsonRpcNotification) -> Result<()> {
    client
        .emit(
            FWS_PEER_NOTIFICATION_EVENT,
            serde_json::to_value(notification)?,
        )
        .map_err(|error| anyhow!("failed to emit FWS peer notification: {error}"))
}

fn shell_payload_value(record: FerrousNativeShellRecord) -> Value {
    json!({
        "id": record.id,
        "spec_id": record.spec_id,
        "backend": record.backend,
        "command": record.command,
        "cwd": record.cwd.map(|path| path.to_string_lossy().into_owned()),
        "pid": record.pid,
        "status": record.status,
        "exit_code": record.exit_code,
        "label": record.label,
        "subgroups": record.subgroups,
        "record_path": record.record_path.to_string_lossy().into_owned(),
        "stdout_log": record.stdout_log.to_string_lossy().into_owned(),
        "stderr_log": record.stderr_log.to_string_lossy().into_owned(),
        "io_metadata_log": record.io_metadata_log.map(|path| path.to_string_lossy().into_owned()),
        "pty_mode": record.pty_mode,
        "autostart": record.autostart,
        "ui": Value::Object(record.ui),
        "debug": Value::Object(record.debug),
        "runtime_id": record.runtime_id,
        "app_id": record.app_id,
        "parent_shell_id": record.parent_shell_id,
        "is_app_worker": record.is_app_worker,
        "capabilities": record.capabilities,
        "adopted": record.adopted,
        "created_at": record.created_at_ms as f64 / 1000.0,
        "updated_at": record.updated_at_ms as f64 / 1000.0,
        "created_at_ms": record.created_at_ms,
        "updated_at_ms": record.updated_at_ms,
        "env_keys": record.env_keys,
        "env_overrides": record.env_overrides,
    })
}

fn socketio_url(controller_url: &str) -> String {
    let trimmed = controller_url.trim_end_matches('/');
    if trimmed.ends_with(FWS_SOCKETIO_SOCKET_PATH) {
        trimmed.to_owned()
    } else {
        format!("{trimmed}{FWS_SOCKETIO_SOCKET_PATH}")
    }
}

fn update_subscriptions(subscriptions: &Arc<Mutex<HashSet<String>>>, payload: Payload) {
    let Some(value) = first_payload_value(&payload) else {
        if let Ok(mut subscriptions) = subscriptions.lock() {
            subscriptions.clear();
        }
        return;
    };
    let parsed = serde_json::from_value::<FwsPeerSubscriptions>(value);
    let Ok(parsed) = parsed else {
        if let Ok(mut subscriptions) = subscriptions.lock() {
            subscriptions.clear();
        }
        return;
    };
    if let Ok(mut subscriptions) = subscriptions.lock() {
        subscriptions.clear();
        subscriptions.extend(
            parsed
                .shell_ids
                .into_iter()
                .map(|shell_id| shell_id.trim().to_owned())
                .filter(|shell_id| !shell_id.is_empty()),
        );
    }
}

fn acknowledge_peer_request(manager: &FerrousNativeManager, payload: Payload, socket: RawClient) {
    let ack_id = payload_ack_id(&payload);
    let response = handle_peer_request(manager, payload);
    if let Some(ack_id) = ack_id {
        let _ = socket.ack_with_id(
            ack_id,
            serde_json::to_value(response).unwrap_or_else(|error| {
                serde_json::json!({
                    "ok": false,
                    "code": "peer_error",
                    "error": error.to_string()
                })
            }),
        );
    }
}

fn handle_peer_request(manager: &FerrousNativeManager, payload: Payload) -> FwsPeerResponse {
    let Some(value) = first_payload_value(&payload) else {
        return peer_error("invalid_request", "Invalid peer request");
    };
    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
    if method != FWS_SHELL_INPUT_METHOD {
        return peer_error(
            "method_not_found",
            format!("Unsupported peer request: {method}"),
        );
    }
    let request = serde_json::from_value::<FwsPeerShellInputRequest>(value);
    let Ok(request) = request else {
        return peer_error("invalid_request", "Invalid peer request");
    };
    let shell_id = request.params.shell_id;
    let result = if request.params.eof {
        manager.send_shell_eof_blocking(&shell_id)
    } else {
        manager.write_to_shell_blocking(
            &shell_id,
            &request.params.data,
            request.params.append_newline,
        )
    };
    match result {
        Ok(result) => peer_success(result),
        Err(error) => peer_error(
            error_code_for_peer_failure(manager, &shell_id, &error),
            error,
        ),
    }
}

fn peer_success(result: FerrousShellInputResult) -> FwsPeerResponse {
    FwsPeerResponse::Success(FwsPeerSuccessResponse::new(Some(
        serde_json::to_value(result).unwrap_or_else(|error| {
            serde_json::json!({
                "serialization_error": error.to_string()
            })
        }),
    )))
}

fn peer_error(code: impl Into<String>, error: impl ToString) -> FwsPeerResponse {
    FwsPeerResponse::Error(FwsPeerErrorResponse::new(code, error.to_string()))
}

fn error_code_for_peer_failure(
    manager: &FerrousNativeManager,
    shell_id: &str,
    error: &anyhow::Error,
) -> &'static str {
    match manager.get_shell(shell_id) {
        Ok(None) => "not_found",
        Ok(Some(_)) => {
            let message = error.to_string();
            if message.contains("not live")
                || message.contains("does not expose")
                || message.contains("unavailable")
            {
                "not_owner"
            } else {
                "write_failed"
            }
        }
        Err(_) => "peer_error",
    }
}

fn first_payload_value(payload: &Payload) -> Option<Value> {
    #[allow(deprecated)]
    match payload {
        Payload::Text(values, _) => values.first().cloned(),
        Payload::String(value, _) => serde_json::from_str(value).ok(),
        Payload::Binary(_, _) => None,
    }
}

fn payload_ack_id(payload: &Payload) -> Option<i32> {
    #[allow(deprecated)]
    match payload {
        Payload::Text(_, ack_id) | Payload::String(_, ack_id) | Payload::Binary(_, ack_id) => {
            *ack_id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_socketio_path_to_base_controller_url() {
        assert_eq!(
            socketio_url("http://127.0.0.1:9000"),
            "http://127.0.0.1:9000/fws_ws/socket.io"
        );
        assert_eq!(
            socketio_url("http://127.0.0.1:9000/"),
            "http://127.0.0.1:9000/fws_ws/socket.io"
        );
        assert_eq!(
            socketio_url("http://127.0.0.1:9000/fws_ws/socket.io"),
            "http://127.0.0.1:9000/fws_ws/socket.io"
        );
    }
}
