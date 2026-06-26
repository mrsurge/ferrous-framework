use crate::shellspec::{
    RenderedReadinessProbe, RenderedShellSpec, ShellspecRenderInput, render_shellspec_entries,
    render_shellspec_entry,
};
use crate::shutdown::{FerrousShutdownResult, FerrousShutdownStats};
use anyhow::{Context, Result, anyhow, bail};
use crossbeam_channel::{
    Receiver as CrossbeamReceiver, Sender as CrossbeamSender,
    TryRecvError as CrossbeamTryRecvError, TrySendError as CrossbeamTrySendError,
    bounded as crossbeam_bounded,
};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    libc,
    poll::{PollFd, PollFlags, PollTimeout, poll},
    pty::openpty,
    unistd::dup,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    fs::{File, OpenOptions, create_dir_all, metadata, read_dir, read_to_string, rename},
    io::{BufWriter, Error as IoError, ErrorKind, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, Sender, TryRecvError, channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::unix::AsyncFd,
    sync::{Mutex as AsyncMutex, broadcast},
};

const FRAMEWORK_SHELLS_SECRET_KEY: &str = "FRAMEWORK_SHELLS_SECRET";
const FRAMEWORK_SHELLS_RUN_ID_KEY: &str = "FRAMEWORK_SHELLS_RUN_ID";
const FRAMEWORK_SHELLS_FWS_SOCKETIO_URL_KEY: &str = "FRAMEWORK_SHELLS_FWS_SOCKETIO_URL";
const TE_FRAMEWORK_URL_KEY: &str = "TE_FRAMEWORK_URL";
const FRAMEWORK_SHELLS_FWS_CHILD_KEY: &str = "FRAMEWORK_SHELLS_FWS_CHILD";
const LEGACY_FWS_CHILD_KEY: &str = "FWSCHILD";
const FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY: &str =
    "FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER";
const MAX_EXITED_SHELLS: usize = 50;
const DIRECT_OUTPUT_READ_CHUNK_BYTES: usize = 64 * 1024;
const DIRECT_OUTPUT_MAX_DRAIN_CHUNKS: usize = 64;
const DIRECT_OUTPUT_LOG_FLUSH_BYTES: usize = 256 * 1024;
const DIRECT_OUTPUT_LOG_FLUSH_INTERVAL_MS: u64 = 250;
static NEXT_GLOBAL_SHELL_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FerrousNativeShellStatus {
    Running,
    Exited,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct FerrousNativeShellCapabilities {
    #[serde(default)]
    pub stdin_write: bool,
    #[serde(default)]
    pub stdin_eof: bool,
    #[serde(default)]
    pub stdout_log: bool,
    #[serde(default)]
    pub stderr_log: bool,
    #[serde(default)]
    pub stdout_subscribe: bool,
    #[serde(default)]
    pub stderr_subscribe: bool,
    #[serde(default)]
    pub output_read: bool,
    #[serde(default)]
    pub terminate: bool,
    #[serde(default)]
    pub resize: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FerrousNativeOutputStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeOutputChunk {
    pub shell_id: String,
    pub stream: FerrousNativeOutputStream,
    pub bytes: Vec<u8>,
    pub dropped_before: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FerrousNativePipeState {
    pub shell_id: String,
    pub backend: String,
    pub pid: u32,
    pub status: FerrousNativeShellStatus,
    pub stdin_supported: bool,
    pub stdout_bytes_seen: u64,
    pub stderr_bytes_seen: u64,
    pub capabilities: FerrousNativeShellCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FerrousShellInputResult {
    pub shell_id: String,
    pub backend: String,
    pub accepted: bool,
    pub bytes_written: usize,
    pub newline_appended: bool,
    pub eof_sent: bool,
}

pub struct FerrousNativeOutputSubscription {
    rx: CrossbeamReceiver<FerrousNativeOutputChunk>,
    stop_tx: CrossbeamSender<()>,
    stop_rx: CrossbeamReceiver<()>,
}

#[derive(Clone)]
pub struct FerrousNativeOutputSubscriptionStopper {
    stop_tx: CrossbeamSender<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FerrousNativeLifecycleEventKind {
    Spawned,
    Updated,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeLifecycleEvent {
    pub kind: FerrousNativeLifecycleEventKind,
    pub shell_id: String,
    pub shell: FerrousNativeShellRecord,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FerrousNativePtyMode {
    Raw,
    #[default]
    Interactive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeShellRecord {
    pub id: String,
    pub backend: String,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub env_keys: Vec<String>,
    pub env_overrides: HashMap<String, String>,
    pub pid: u32,
    pub status: FerrousNativeShellStatus,
    pub exit_code: Option<i32>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub record_path: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub io_metadata_log: Option<PathBuf>,
    pub pty_mode: Option<FerrousNativePtyMode>,
    pub autostart: bool,
    pub ui: Map<String, Value>,
    pub debug: Map<String, Value>,
    pub runtime_id: Option<String>,
    pub app_id: Option<String>,
    pub parent_shell_id: Option<String>,
    pub is_app_worker: bool,
    pub capabilities: FerrousNativeShellCapabilities,
    pub adopted: bool,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeEnv {
    pub secret: String,
    pub run_id: String,
    pub fws_socketio_url: Option<String>,
    pub te_framework_url: Option<String>,
    pub extra: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeStore {
    pub secret: String,
    pub runtime_id: String,
    pub repo_fingerprint: String,
    pub base_dir: PathBuf,
    pub root: PathBuf,
    pub metadata_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub sockets_dir: PathBuf,
    pub secret_file: PathBuf,
}

#[derive(Clone, Debug)]
pub struct FerrousNativeProcConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct FerrousNativePipeConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct FerrousNativePtyConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: Option<PathBuf>,
    pub mode: FerrousNativePtyMode,
}

#[derive(Clone, Debug, Default)]
pub struct FerrousShellLaunchOverrides {
    pub env: HashMap<String, String>,
    pub label: Option<String>,
    pub spec_id: Option<String>,
    pub subgroups: Option<Vec<String>>,
    pub ui: Option<Map<String, Value>>,
    pub debug: Option<Map<String, Value>>,
    pub parent_shell_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct FerrousPipeConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: Option<PathBuf>,
    pub shellspec_path: Option<PathBuf>,
    pub shellspec_entry: Option<String>,
    pub backend: Option<String>,
    pub ctx: HashMap<String, String>,
    pub python_module: Option<String>,
    pub python_class: Option<String>,
}

#[derive(Clone)]
pub struct FerrousNativeManager {
    state: Arc<Mutex<ManagerState>>,
    subscriptions: Arc<Mutex<OutputSubscriptions>>,
    subscriber_count: Arc<AtomicU64>,
    async_outputs: Arc<Mutex<HashMap<String, Arc<AsyncDirectOutput>>>>,
    lifecycle_tx: broadcast::Sender<FerrousNativeLifecycleEvent>,
    reactor: NativeRuntimeReactor,
    store: FerrousNativeStore,
    native_env: FerrousNativeEnv,
    parent_peer: Arc<Mutex<Option<crate::native_peer::FerrousNativePeer>>>,
}

#[derive(Clone)]
pub struct FerrousFrameworkPipe {
    manager: FerrousNativeManager,
    shell_id: String,
}

#[derive(Default)]
struct ManagerState {
    next_id: u64,
    entries: HashMap<String, NativeShellEntry>,
}

#[derive(Default)]
struct OutputSubscriptions {
    subscribers: HashMap<(String, FerrousNativeOutputStream), Vec<OutputSubscriber>>,
    dropped: HashMap<(String, FerrousNativeOutputStream), u64>,
}

struct OutputSubscriber {
    tx: CrossbeamSender<FerrousNativeOutputChunk>,
}

struct NativeShellEntry {
    record: FerrousNativeShellRecord,
    record_path: PathBuf,
    child: Arc<Mutex<Child>>,
    input: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    output: Option<Arc<Mutex<DirectOutput>>>,
    resize: Option<Arc<File>>,
}

#[derive(Clone, Debug, Default)]
struct SpawnRecordMetadata {
    env_overrides: HashMap<String, String>,
    ui: Map<String, Value>,
    debug: Map<String, Value>,
    parent_shell_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedNativeShellRecord {
    id: String,
    #[serde(default)]
    spec_id: Option<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    command: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    status: Option<FerrousNativeShellStatus>,
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    subgroups: Vec<String>,
    #[serde(default)]
    record_path: Option<String>,
    #[serde(default)]
    stdout_log: Option<String>,
    #[serde(default)]
    stderr_log: Option<String>,
    #[serde(default)]
    io_metadata_log: Option<String>,
    #[serde(default)]
    pty_mode: Option<FerrousNativePtyMode>,
    #[serde(default = "default_true")]
    autostart: bool,
    #[serde(default)]
    ui: Map<String, Value>,
    #[serde(default)]
    debug: Map<String, Value>,
    #[serde(default)]
    created_at_ms: u128,
    #[serde(default)]
    updated_at_ms: u128,
    #[serde(default)]
    created_at: Option<f64>,
    #[serde(default)]
    updated_at: Option<f64>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    launcher_pid: Option<u32>,
    #[serde(default)]
    env_keys: Vec<String>,
    #[serde(default)]
    env_overrides: Map<String, Value>,
    #[serde(default)]
    uses_pty: bool,
    #[serde(default)]
    uses_pipes: bool,
    #[serde(default)]
    uses_dtach: bool,
    #[serde(default)]
    runtime_id: Option<String>,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default)]
    app_id: Option<String>,
    #[serde(default)]
    parent_shell_id: Option<String>,
    #[serde(default)]
    is_app_worker: bool,
    #[serde(default)]
    capabilities: FerrousNativeShellCapabilities,
}

struct DirectOutput {
    shell_id: String,
    stream: FerrousNativeOutputStream,
    reader: Box<dyn ReadAsFd>,
    log: BufWriter<File>,
    line_buffer: Vec<u8>,
    pending_flush_bytes: usize,
    last_flush_at: Instant,
    subscriptions: Arc<Mutex<OutputSubscriptions>>,
    subscriber_count: Arc<AtomicU64>,
}

struct AsyncDirectOutput {
    shell_id: String,
    stream: FerrousNativeOutputStream,
    reader: AsyncFd<OwnedFd>,
    log: AsyncMutex<AsyncOutputLog>,
    subscriptions: Arc<Mutex<OutputSubscriptions>>,
    subscriber_count: Arc<AtomicU64>,
}

struct AsyncOutputLog {
    log: BufWriter<File>,
    pending_flush_bytes: usize,
    last_flush_at: Instant,
}

trait ReadAsFd: Read + AsRawFd + Send {}
impl<T: Read + AsRawFd + Send> ReadAsFd for T {}

#[derive(Clone)]
struct NativeRuntimeReactor {
    tx: Sender<ReactorCommand>,
    subscriber_count: Arc<AtomicU64>,
}

enum ReactorCommand {
    RegisterLogStream(ReactorLogStream),
    WatchChild(ReactorChild),
}

struct ReactorLogStream {
    shell_id: String,
    stream: FerrousNativeOutputStream,
    reader: Box<dyn ReadAsFd>,
    log: BufWriter<File>,
    subscriber_count: Arc<AtomicU64>,
}

struct ReactorChild {
    shell_id: String,
    child: Arc<Mutex<Child>>,
}

#[derive(Clone, Debug)]
struct ProcfsEntry {
    pid: i64,
    ppid: i64,
    state: char,
}

impl NativeRuntimeReactor {
    fn start(
        state: Arc<Mutex<ManagerState>>,
        subscriptions: Arc<Mutex<OutputSubscriptions>>,
        subscriber_count: Arc<AtomicU64>,
        lifecycle_tx: broadcast::Sender<FerrousNativeLifecycleEvent>,
    ) -> Self {
        let (tx, rx) = channel::<ReactorCommand>();
        thread::spawn(move || reactor_loop(state, subscriptions, lifecycle_tx, rx));
        Self {
            tx,
            subscriber_count,
        }
    }

    fn register_log_stream(
        &self,
        shell_id: String,
        stream: FerrousNativeOutputStream,
        reader: impl ReadAsFd + 'static,
        file: File,
    ) -> Result<()> {
        self.tx
            .send(ReactorCommand::RegisterLogStream(ReactorLogStream {
                shell_id,
                stream,
                reader: Box::new(reader),
                log: BufWriter::with_capacity(256 * 1024, file),
                subscriber_count: Arc::clone(&self.subscriber_count),
            }))
            .map_err(|_| anyhow!("native runtime reactor stopped"))
    }

    fn watch_child(&self, shell_id: String, child: Arc<Mutex<Child>>) -> Result<()> {
        self.tx
            .send(ReactorCommand::WatchChild(ReactorChild { shell_id, child }))
            .map_err(|_| anyhow!("native runtime reactor stopped"))
    }
}

impl FerrousNativeOutputSubscription {
    pub fn stopper(&self) -> FerrousNativeOutputSubscriptionStopper {
        FerrousNativeOutputSubscriptionStopper {
            stop_tx: self.stop_tx.clone(),
        }
    }

    pub fn recv(&self) -> Result<Option<FerrousNativeOutputChunk>> {
        crossbeam_channel::select! {
            recv(self.rx) -> chunk => match chunk {
                Ok(chunk) => Ok(Some(chunk)),
                Err(_) => Ok(None),
            },
            recv(self.stop_rx) -> _ => Ok(None),
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<FerrousNativeOutputChunk>> {
        crossbeam_channel::select! {
            recv(self.rx) -> chunk => match chunk {
                Ok(chunk) => Ok(Some(chunk)),
                Err(_) => Ok(None),
            },
            recv(self.stop_rx) -> _ => Ok(None),
            default(timeout) => Ok(None),
        }
    }

    pub fn try_recv(&self) -> Result<Option<FerrousNativeOutputChunk>> {
        match self.stop_rx.try_recv() {
            Ok(()) | Err(CrossbeamTryRecvError::Disconnected) => return Ok(None),
            Err(CrossbeamTryRecvError::Empty) => {}
        }
        match self.rx.try_recv() {
            Ok(chunk) => Ok(Some(chunk)),
            Err(CrossbeamTryRecvError::Empty) | Err(CrossbeamTryRecvError::Disconnected) => {
                Ok(None)
            }
        }
    }
}

impl FerrousNativeOutputSubscriptionStopper {
    pub fn stop(&self) {
        let _ = self.stop_tx.try_send(());
    }
}

impl FerrousNativeManager {
    pub fn new() -> Self {
        Self::try_new().expect("failed to initialize native FWS store")
    }

    pub fn try_new() -> Result<Self> {
        let store = FerrousNativeStore::from_process_env()?;
        let native_env = FerrousNativeEnv::from_process_env_with_secret(store.secret.clone());
        Ok(Self::with_store_and_env(store, native_env))
    }

    pub fn with_env(native_env: FerrousNativeEnv) -> Self {
        let store = FerrousNativeStore::from_secret(native_env.secret.clone())
            .expect("failed to initialize native FWS store");
        Self::with_store_and_env(store, native_env)
    }

    pub fn try_with_env_map(env_map: &HashMap<String, String>) -> Result<Self> {
        let store = FerrousNativeStore::from_env_map(env_map)?;
        let native_env = FerrousNativeEnv::from_env_map_with_secret(env_map, store.secret.clone());
        Ok(Self::with_store_and_env(store, native_env))
    }

    pub fn with_env_map(env_map: &HashMap<String, String>) -> Self {
        Self::try_with_env_map(env_map).expect("failed to initialize native FWS store from env map")
    }

    pub fn with_store_and_env(store: FerrousNativeStore, native_env: FerrousNativeEnv) -> Self {
        Self::with_store_and_env_options(store, native_env, true)
    }

    pub(crate) fn with_store_and_env_without_parent_peer(
        store: FerrousNativeStore,
        native_env: FerrousNativeEnv,
    ) -> Self {
        Self::with_store_and_env_options(store, native_env, false)
    }

    fn with_store_and_env_options(
        store: FerrousNativeStore,
        native_env: FerrousNativeEnv,
        start_parent_peer: bool,
    ) -> Self {
        let state = Arc::new(Mutex::new(ManagerState::default()));
        let subscriptions = Arc::new(Mutex::new(OutputSubscriptions::default()));
        let subscriber_count = Arc::new(AtomicU64::new(0));
        let async_outputs = Arc::new(Mutex::new(HashMap::new()));
        let (lifecycle_tx, _) = broadcast::channel(1024);
        let reactor = NativeRuntimeReactor::start(
            Arc::clone(&state),
            Arc::clone(&subscriptions),
            Arc::clone(&subscriber_count),
            lifecycle_tx.clone(),
        );
        let manager = Self {
            state,
            subscriptions,
            subscriber_count,
            async_outputs,
            lifecycle_tx,
            reactor,
            store,
            native_env,
            parent_peer: Arc::new(Mutex::new(None)),
        };
        if start_parent_peer {
            manager.start_parent_peer_if_requested();
        }
        manager
    }

    fn clone_without_parent_peer(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            subscriptions: Arc::clone(&self.subscriptions),
            subscriber_count: Arc::clone(&self.subscriber_count),
            async_outputs: Arc::clone(&self.async_outputs),
            lifecycle_tx: self.lifecycle_tx.clone(),
            reactor: self.reactor.clone(),
            store: self.store.clone(),
            native_env: self.native_env.clone(),
            parent_peer: Arc::new(Mutex::new(None)),
        }
    }

    fn start_parent_peer_if_requested(&self) {
        if !native_env_requests_parent_peer(&self.native_env) {
            return;
        }
        if native_env_disables_parent_peer(&self.native_env) {
            return;
        }
        let peer_manager = self.clone_without_parent_peer();
        match crate::native_peer::FerrousNativePeer::connect_from_manager_env(peer_manager) {
            Ok(peer) => {
                if let Ok(mut parent_peer) = self.parent_peer.lock() {
                    *parent_peer = Some(peer);
                }
            }
            Err(error) => {
                eprintln!("[ferrous-framework] fws parent peer connect failed: {error:#}");
            }
        }
    }

    pub fn store(&self) -> FerrousNativeStore {
        self.store.clone()
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.store.logs_dir.clone()
    }

    pub fn native_env(&self) -> FerrousNativeEnv {
        self.native_env.clone()
    }

    pub fn child_env_overlay(&self) -> HashMap<String, String> {
        self.native_env.child_env_overlay()
    }

    pub async fn spawn_shell(
        &self,
        config: FerrousNativeProcConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let manager = self.clone();
        tokio::task::spawn_blocking(move || manager.spawn_proc_blocking(config))
            .await
            .context("native proc spawn task failed")?
    }

    pub async fn spawn_shell_pipe(
        &self,
        config: FerrousNativePipeConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let manager = self.clone();
        tokio::task::spawn_blocking(move || manager.spawn_pipe_blocking(config))
            .await
            .context("native pipe spawn task failed")?
    }

    pub async fn spawn_shell_pty(
        &self,
        config: FerrousNativePtyConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let manager = self.clone();
        tokio::task::spawn_blocking(move || manager.spawn_pty_blocking(config))
            .await
            .context("native PTY spawn task failed")?
    }

    pub async fn write_to_pipe(&self, shell_id: &str, data: &str) -> Result<()> {
        self.write_to_pipe_blocking(shell_id, data.as_bytes())
    }

    pub async fn write_to_shell(
        &self,
        shell_id: &str,
        data: &str,
        append_newline: bool,
    ) -> Result<FerrousShellInputResult> {
        self.write_to_shell_blocking(shell_id, data, append_newline)
    }

    pub async fn send_shell_eof(&self, shell_id: &str) -> Result<FerrousShellInputResult> {
        self.send_shell_eof_blocking(shell_id)
    }

    pub async fn terminate_shell(&self, shell_id: &str, force: bool) -> Result<()> {
        let manager = self.clone();
        let shell_id = shell_id.to_owned();
        tokio::task::spawn_blocking(move || {
            manager.terminate_shell_strict_blocking(&shell_id, force)
        })
        .await
        .context("native shell terminate task failed")?
    }

    pub async fn subscribe_output_bytes(
        &self,
        shell_id: &str,
        capacity: usize,
    ) -> Result<Option<FerrousNativeOutputSubscription>> {
        self.subscribe_output(shell_id, FerrousNativeOutputStream::Stdout, capacity)
    }

    pub async fn read_stdout_available(
        &self,
        shell_id: &str,
        max_chunks: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let output = self.async_stdout_output(shell_id)?;
        output.read_available(max_chunks).await
    }

    pub fn subscribe_lifecycle(&self) -> broadcast::Receiver<FerrousNativeLifecycleEvent> {
        self.lifecycle_tx.subscribe()
    }

    pub fn spawn_shellspec_entry_blocking(
        &self,
        document: &Value,
        entry: &str,
        input: &ShellspecRenderInput,
    ) -> Result<FerrousNativeShellRecord> {
        let spec = render_shellspec_entry(document, entry, input)?;
        self.spawn_rendered_shellspec_blocking(spec)
    }

    pub fn spawn_shellspec_entry_with_overrides_blocking(
        &self,
        document: &Value,
        entry: &str,
        input: &ShellspecRenderInput,
        overrides: FerrousShellLaunchOverrides,
    ) -> Result<FerrousNativeShellRecord> {
        let spec = render_shellspec_entry(document, entry, input)?;
        self.spawn_rendered_shellspec_with_overrides_blocking(spec, overrides)
    }

    pub fn spawn_rendered_shellspec_blocking(
        &self,
        spec: RenderedShellSpec,
    ) -> Result<FerrousNativeShellRecord> {
        self.spawn_rendered_shellspec_with_options_blocking(
            spec,
            None,
            FerrousShellLaunchOverrides::default(),
        )
    }

    pub fn spawn_rendered_shellspec_with_overrides_blocking(
        &self,
        spec: RenderedShellSpec,
        overrides: FerrousShellLaunchOverrides,
    ) -> Result<FerrousNativeShellRecord> {
        self.spawn_rendered_shellspec_with_options_blocking(spec, None, overrides)
    }

    pub fn spawn_rendered_shellspec_with_log_dir_blocking(
        &self,
        spec: RenderedShellSpec,
        log_dir: Option<PathBuf>,
    ) -> Result<FerrousNativeShellRecord> {
        self.spawn_rendered_shellspec_with_options_blocking(
            spec,
            log_dir,
            FerrousShellLaunchOverrides::default(),
        )
    }

    fn spawn_rendered_shellspec_with_options_blocking(
        &self,
        spec: RenderedShellSpec,
        log_dir: Option<PathBuf>,
        overrides: FerrousShellLaunchOverrides,
    ) -> Result<FerrousNativeShellRecord> {
        if !spec.autostart {
            bail!("shellspec '{}' has autostart=false", spec.id);
        }
        let label = overrides.label.unwrap_or_else(|| spec.id.clone());
        let spec_id = overrides.spec_id.unwrap_or_else(|| spec.id.clone());
        let subgroups = overrides
            .subgroups
            .unwrap_or_else(|| spec.subgroups.clone());
        let mut env = spec.env.clone();
        env.extend(overrides.env);
        let mut ui = spec.ui.clone();
        if let Some(override_ui) = overrides.ui {
            ui.extend(override_ui);
        }
        let mut debug = spec.debug.clone();
        if let Some(override_debug) = overrides.debug {
            debug.extend(override_debug);
        }
        let metadata = SpawnRecordMetadata {
            env_overrides: env.clone(),
            ui,
            debug,
            parent_shell_id: overrides.parent_shell_id,
        };
        let readiness = spec.readiness.clone();
        let record = match spec.backend.as_str() {
            "proc" => self.spawn_proc_with_metadata_blocking(
                FerrousNativeProcConfig {
                    command: spec.command,
                    cwd: spec.cwd,
                    env,
                    label,
                    spec_id,
                    subgroups,
                    log_dir: log_dir.clone(),
                },
                metadata,
            ),
            "pipe" => self.spawn_pipe_with_metadata_blocking(
                FerrousNativePipeConfig {
                    command: spec.command,
                    cwd: spec.cwd,
                    env,
                    label,
                    spec_id,
                    subgroups,
                    log_dir: log_dir.clone(),
                },
                metadata,
            ),
            "pty" => self.spawn_pty_with_metadata_blocking(
                FerrousNativePtyConfig {
                    command: spec.command,
                    cwd: spec.cwd,
                    env,
                    label,
                    spec_id,
                    subgroups,
                    log_dir,
                    mode: parse_pty_mode(&spec.pty_mode)?,
                },
                metadata,
            ),
            backend => bail!("unsupported native shellspec backend '{backend}'"),
        }?;
        if let Some(probe) = readiness {
            if !wait_for_readiness_blocking(&record, &probe)? {
                let _ = self.terminate_shell_blocking(&record.id);
                bail!(
                    "shellspec '{}' failed readiness ({})",
                    record.spec_id,
                    probe.probe_type
                );
            }
        }
        Ok(record)
    }

    pub fn apply_shellspec_document_blocking(
        &self,
        document: &Value,
        input: &ShellspecRenderInput,
        prune: bool,
    ) -> Result<Vec<FerrousNativeShellRecord>> {
        let specs = render_shellspec_entries(document, input)?;
        self.apply_rendered_shellspecs_blocking(specs, prune)
    }

    pub fn apply_rendered_shellspecs_blocking(
        &self,
        specs: Vec<RenderedShellSpec>,
        prune: bool,
    ) -> Result<Vec<FerrousNativeShellRecord>> {
        let desired_ids = specs
            .iter()
            .map(|spec| spec.id.clone())
            .collect::<HashSet<_>>();
        let mut started = Vec::new();
        for spec in specs {
            if !spec.autostart {
                continue;
            }
            if self.live_running_record_by_spec_id(&spec.id)?.is_some() {
                continue;
            }
            started.push(self.spawn_rendered_shellspec_blocking(spec)?);
        }
        if prune {
            for record in self.live_records()? {
                if !desired_ids.contains(&record.spec_id) {
                    let _ = self.terminate_shell_blocking(&record.id)?;
                }
            }
        }
        Ok(started)
    }

    pub fn spawn_proc_blocking(
        &self,
        config: FerrousNativeProcConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let metadata = SpawnRecordMetadata {
            env_overrides: config.env.clone(),
            ..SpawnRecordMetadata::default()
        };
        self.spawn_proc_with_metadata_blocking(config, metadata)
    }

    fn spawn_proc_with_metadata_blocking(
        &self,
        config: FerrousNativeProcConfig,
        metadata: SpawnRecordMetadata,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native proc")?;
        let log_dir = self.resolve_log_dir(config.log_dir)?;
        create_dir_all(&log_dir).with_context(|| {
            format!(
                "failed to create native proc log directory {}",
                log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = log_dir.join(format!("{id}.stderr.log"));
        let record_path = record_path_for(&self.store.metadata_dir, &id);
        let stdout_file = File::create(&stdout_log)
            .with_context(|| format!("failed to create stdout log {}", stdout_log.display()))?;
        let stderr_file = File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;
        let child_env = self.merged_child_env(config.env);

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&child_env);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn native proc shell {id}"))?;
        let pid = child.id();
        if let Some(stdout) = child.stdout.take() {
            set_nonblocking(&stdout)?;
            self.reactor.register_log_stream(
                id.clone(),
                FerrousNativeOutputStream::Stdout,
                stdout,
                stdout_file,
            )?;
        }
        if let Some(stderr) = child.stderr.take() {
            set_nonblocking(&stderr)?;
            self.reactor.register_log_stream(
                id.clone(),
                FerrousNativeOutputStream::Stderr,
                stderr,
                stderr_file,
            )?;
        }

        let now = now_ms();
        let io_metadata_log = Some(io_metadata_path_for(&log_dir, &id));
        let app_id = derive_app_id(&config.label, &config.subgroups);
        let is_app_worker = derive_is_app_worker(&config.label, &config.subgroups);
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "proc".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            env_overrides: metadata.env_overrides,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            io_metadata_log,
            pty_mode: None,
            autostart: true,
            ui: metadata.ui,
            debug: metadata.debug,
            runtime_id: Some(self.store.runtime_id.clone()),
            app_id,
            parent_shell_id: metadata.parent_shell_id,
            is_app_worker,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: false,
                stdin_eof: false,
                stdout_log: true,
                stderr_log: true,
                stdout_subscribe: true,
                stderr_subscribe: true,
                output_read: false,
                terminate: true,
                resize: false,
            },
            adopted: false,
            created_at_ms: now,
            updated_at_ms: now,
        };
        persist_record(&record, &record_path)?;
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(
            id.clone(),
            record.clone(),
            record_path,
            Arc::clone(&child),
            None,
            None,
            None,
        )?;
        self.reactor.watch_child(id, child)?;
        self.publish_lifecycle(FerrousNativeLifecycleEventKind::Spawned, record.clone());
        Ok(record)
    }

    pub fn spawn_pipe_blocking(
        &self,
        config: FerrousNativePipeConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let metadata = SpawnRecordMetadata {
            env_overrides: config.env.clone(),
            ..SpawnRecordMetadata::default()
        };
        self.spawn_pipe_with_metadata_blocking(config, metadata)
    }

    fn spawn_pipe_with_metadata_blocking(
        &self,
        config: FerrousNativePipeConfig,
        metadata: SpawnRecordMetadata,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native pipe")?;
        let log_dir = self.resolve_log_dir(config.log_dir)?;
        create_dir_all(&log_dir).with_context(|| {
            format!(
                "failed to create native pipe log directory {}",
                log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = log_dir.join(format!("{id}.stderr.log"));
        let record_path = record_path_for(&self.store.metadata_dir, &id);
        let stdout_file = open_log(&stdout_log)?;
        let stderr_file = File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;
        let child_env = self.merged_child_env(config.env);

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&child_env);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn native pipe shell {id}"))?;
        let pid = child.id();
        let input = child.stdin.take().map(boxed_writer);
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("native pipe stdout missing"))?;
        set_nonblocking(&stdout)?;
        let output = Some(Arc::new(Mutex::new(DirectOutput {
            shell_id: id.clone(),
            stream: FerrousNativeOutputStream::Stdout,
            reader: Box::new(stdout),
            log: BufWriter::with_capacity(256 * 1024, stdout_file),
            line_buffer: Vec::new(),
            pending_flush_bytes: 0,
            last_flush_at: Instant::now(),
            subscriptions: Arc::clone(&self.subscriptions),
            subscriber_count: Arc::clone(&self.subscriber_count),
        })));
        if let Some(stderr) = child.stderr.take() {
            set_nonblocking(&stderr)?;
            self.reactor.register_log_stream(
                id.clone(),
                FerrousNativeOutputStream::Stderr,
                stderr,
                stderr_file,
            )?;
        }

        let now = now_ms();
        let io_metadata_log = Some(io_metadata_path_for(&log_dir, &id));
        let app_id = derive_app_id(&config.label, &config.subgroups);
        let is_app_worker = derive_is_app_worker(&config.label, &config.subgroups);
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "pipe".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            env_overrides: metadata.env_overrides,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            io_metadata_log,
            pty_mode: None,
            autostart: true,
            ui: metadata.ui,
            debug: metadata.debug,
            runtime_id: Some(self.store.runtime_id.clone()),
            app_id,
            parent_shell_id: metadata.parent_shell_id,
            is_app_worker,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: input.is_some(),
                stdin_eof: input.is_some(),
                stdout_log: true,
                stderr_log: true,
                stdout_subscribe: true,
                stderr_subscribe: true,
                output_read: true,
                terminate: true,
                resize: false,
            },
            adopted: false,
            created_at_ms: now,
            updated_at_ms: now,
        };
        persist_record(&record, &record_path)?;
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(
            id.clone(),
            record.clone(),
            record_path,
            Arc::clone(&child),
            input,
            output,
            None,
        )?;
        self.reactor.watch_child(id, child)?;
        self.publish_lifecycle(FerrousNativeLifecycleEventKind::Spawned, record.clone());
        Ok(record)
    }

    pub fn spawn_pty_blocking(
        &self,
        config: FerrousNativePtyConfig,
    ) -> Result<FerrousNativeShellRecord> {
        let metadata = SpawnRecordMetadata {
            env_overrides: config.env.clone(),
            ..SpawnRecordMetadata::default()
        };
        self.spawn_pty_with_metadata_blocking(config, metadata)
    }

    fn spawn_pty_with_metadata_blocking(
        &self,
        config: FerrousNativePtyConfig,
        metadata: SpawnRecordMetadata,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native pty")?;
        let log_dir = self.resolve_log_dir(config.log_dir)?;
        create_dir_all(&log_dir).with_context(|| {
            format!(
                "failed to create native pty log directory {}",
                log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = log_dir.join(format!("{id}.stderr.log"));
        let record_path = record_path_for(&self.store.metadata_dir, &id);
        let stdout_file = open_log(&stdout_log)?;
        File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;

        let pty = openpty(None, None).context("failed to open PTY")?;
        apply_pty_mode(pty.slave.as_raw_fd(), config.mode)?;
        let slave_stdin = dup(&pty.slave).context("failed to duplicate PTY slave stdin")?;
        let slave_stdout = dup(&pty.slave).context("failed to duplicate PTY slave stdout")?;
        let slave_stderr = pty.slave;
        let master_input = dup(&pty.master).context("failed to duplicate PTY master input")?;
        let master_resize = dup(&pty.master).context("failed to duplicate PTY master resize fd")?;
        let master_output = File::from(pty.master);
        set_nonblocking(&master_output)?;
        let child_env = self.merged_child_env(config.env);

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&child_env);
        command
            .stdin(Stdio::from(File::from(slave_stdin)))
            .stdout(Stdio::from(File::from(slave_stdout)))
            .stderr(Stdio::from(File::from(slave_stderr)));
        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn native pty shell {id}"))?;
        let pid = child.id();
        let input = Some(boxed_writer(File::from(master_input)));
        let output = Some(Arc::new(Mutex::new(DirectOutput {
            shell_id: id.clone(),
            stream: FerrousNativeOutputStream::Stdout,
            reader: Box::new(master_output),
            log: BufWriter::with_capacity(256 * 1024, stdout_file),
            line_buffer: Vec::new(),
            pending_flush_bytes: 0,
            last_flush_at: Instant::now(),
            subscriptions: Arc::clone(&self.subscriptions),
            subscriber_count: Arc::clone(&self.subscriber_count),
        })));

        let now = now_ms();
        let io_metadata_log = Some(io_metadata_path_for(&log_dir, &id));
        let app_id = derive_app_id(&config.label, &config.subgroups);
        let is_app_worker = derive_is_app_worker(&config.label, &config.subgroups);
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "pty".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            env_overrides: metadata.env_overrides,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            io_metadata_log,
            pty_mode: Some(config.mode),
            autostart: true,
            ui: metadata.ui,
            debug: metadata.debug,
            runtime_id: Some(self.store.runtime_id.clone()),
            app_id,
            parent_shell_id: metadata.parent_shell_id,
            is_app_worker,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: true,
                stdin_eof: true,
                stdout_log: true,
                stderr_log: false,
                stdout_subscribe: true,
                stderr_subscribe: false,
                output_read: true,
                terminate: true,
                resize: true,
            },
            adopted: false,
            created_at_ms: now,
            updated_at_ms: now,
        };
        persist_record(&record, &record_path)?;
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(
            id.clone(),
            record.clone(),
            record_path,
            Arc::clone(&child),
            input,
            output,
            Some(Arc::new(File::from(master_resize))),
        )?;
        self.reactor.watch_child(id, child)?;
        self.publish_lifecycle(FerrousNativeLifecycleEventKind::Spawned, record.clone());
        Ok(record)
    }

    pub fn list_shells(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        let mut records = self
            .list_persisted_records()?
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let state = self.lock_state()?;
        for entry in state.entries.values() {
            records.insert(entry.record.id.clone(), entry.record.clone());
        }
        Ok(limit_exited_shell_history(records.into_values().collect()))
    }

    pub fn get_shell(&self, shell_id: &str) -> Result<Option<FerrousNativeShellRecord>> {
        {
            let state = self.lock_state()?;
            if let Some(entry) = state.entries.get(shell_id) {
                return Ok(Some(entry.record.clone()));
            }
        }
        let record_path = record_path_for(&self.store.metadata_dir, shell_id);
        if record_path.exists() {
            return Ok(Some(load_persisted_record(&record_path)?));
        }
        Ok(None)
    }

    pub fn get_pipe_state(&self, shell_id: &str) -> Result<Option<FerrousNativePipeState>> {
        let (record, stdin_supported) = {
            let state = self.lock_state()?;
            let Some(entry) = state.entries.get(shell_id) else {
                return Ok(None);
            };
            if entry.record.backend != "pipe" && entry.record.backend != "pty" {
                return Ok(None);
            }
            (
                entry.record.clone(),
                entry.input.is_some() && entry.record.status == FerrousNativeShellStatus::Running,
            )
        };
        Ok(Some(FerrousNativePipeState {
            shell_id: record.id,
            backend: record.backend,
            pid: record.pid,
            status: record.status,
            stdin_supported,
            stdout_bytes_seen: file_len(&record.stdout_log),
            stderr_bytes_seen: file_len(&record.stderr_log),
            capabilities: record.capabilities,
        }))
    }

    pub fn list_persisted_records(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        list_persisted_records_in_dir(&self.store.metadata_dir)
    }

    pub fn live_records(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        let state = self.lock_state()?;
        let mut records = state
            .entries
            .values()
            .map(|entry| entry.record.clone())
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }

    pub fn terminate_shell_blocking(&self, shell_id: &str) -> Result<bool> {
        let child = {
            let state = self.lock_state()?;
            if let Some(entry) = state.entries.get(shell_id) {
                if entry.record.status == FerrousNativeShellStatus::Exited {
                    return Ok(true);
                }
                Some(Arc::clone(&entry.child))
            } else {
                None
            }
        };
        let Some(child) = child else {
            return self.terminate_persisted_shell_blocking(shell_id, libc::SIGKILL);
        };
        let mut child = child
            .lock()
            .map_err(|_| anyhow!("native child lock poisoned"))?;
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        Ok(true)
    }

    fn terminate_persisted_shell_blocking(
        &self,
        shell_id: &str,
        signal: libc::c_int,
    ) -> Result<bool> {
        let Some(record) = self.get_shell(shell_id)? else {
            return Ok(false);
        };
        if record.status == FerrousNativeShellStatus::Exited {
            return Ok(true);
        }
        let pid = i64::from(record.pid);
        if !pid_is_live_i64(pid) {
            self.mark_record_exited(&record, None)?;
            return Ok(true);
        }
        if signal_pid_or_process_group(pid, signal)? {
            let deadline = Instant::now() + Duration::from_secs(1);
            while pid_is_live_i64(pid) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
            }
        }
        if pid_is_live_i64(pid) {
            bail!("native shell {shell_id} pid {pid} did not exit after signal {signal}");
        }
        self.mark_record_exited(&record, Some(-signal))?;
        Ok(true)
    }

    pub fn terminate_shell_strict_blocking(&self, shell_id: &str, _force: bool) -> Result<()> {
        if self.terminate_shell_blocking(shell_id)? {
            Ok(())
        } else {
            bail!("native shell {shell_id} not found or not live")
        }
    }

    pub fn write_line_blocking(&self, shell_id: &str, line: &str) -> Result<bool> {
        self.write_blocking(shell_id, line.as_bytes())?;
        self.write_blocking(shell_id, b"\n")
    }

    pub fn write_line_strict_blocking(&self, shell_id: &str, line: &str) -> Result<()> {
        self.write_to_shell_blocking(shell_id, line, true)?;
        Ok(())
    }

    pub fn write_blocking(&self, shell_id: &str, bytes: &[u8]) -> Result<bool> {
        let input = {
            let state = self.lock_state()?;
            let Some(entry) = state.entries.get(shell_id) else {
                return Ok(false);
            };
            entry.input.clone()
        };
        let Some(input) = input else {
            bail!("native shell {shell_id} does not expose stdin");
        };
        let mut input = input
            .lock()
            .map_err(|_| anyhow!("native input lock poisoned"))?;
        input.write_all(bytes)?;
        input.flush()?;
        Ok(true)
    }

    pub fn write_to_pipe_blocking(&self, shell_id: &str, bytes: &[u8]) -> Result<()> {
        if self.write_blocking(shell_id, bytes)? {
            Ok(())
        } else {
            bail!("native shell {shell_id} not found or not live")
        }
    }

    pub fn write_to_shell_blocking(
        &self,
        shell_id: &str,
        data: &str,
        append_newline: bool,
    ) -> Result<FerrousShellInputResult> {
        let backend = self.live_shell_backend(shell_id)?;
        let mut bytes = data.as_bytes().to_vec();
        if append_newline {
            bytes.push(b'\n');
        }
        self.write_to_pipe_blocking(shell_id, &bytes)?;
        Ok(FerrousShellInputResult {
            shell_id: shell_id.to_owned(),
            backend,
            accepted: true,
            bytes_written: bytes.len(),
            newline_appended: append_newline,
            eof_sent: false,
        })
    }

    pub fn subscribe_output(
        &self,
        shell_id: &str,
        stream: FerrousNativeOutputStream,
        capacity: usize,
    ) -> Result<Option<FerrousNativeOutputSubscription>> {
        if capacity == 0 {
            bail!("native output subscription capacity must be greater than zero");
        }
        {
            let state = self.lock_state()?;
            let Some(entry) = state.entries.get(shell_id) else {
                return Ok(None);
            };
            let can_subscribe = match stream {
                FerrousNativeOutputStream::Stdout => entry.record.capabilities.stdout_subscribe,
                FerrousNativeOutputStream::Stderr => entry.record.capabilities.stderr_subscribe,
            };
            if !can_subscribe {
                bail!(
                    "native shell {shell_id} does not expose {:?} subscription",
                    stream
                );
            }
        }
        let (tx, rx) = crossbeam_bounded(capacity);
        let (stop_tx, stop_rx) = crossbeam_bounded(1);
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| anyhow!("native output subscription lock poisoned"))?;
        subscriptions
            .subscribers
            .entry((shell_id.to_owned(), stream))
            .or_default()
            .push(OutputSubscriber { tx });
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        Ok(Some(FerrousNativeOutputSubscription {
            rx,
            stop_tx,
            stop_rx,
        }))
    }

    pub fn send_stdin_eof_blocking(&self, shell_id: &str) -> Result<bool> {
        let (input, updated_record) = {
            let mut state = self.lock_state()?;
            let Some(entry) = state.entries.get_mut(shell_id) else {
                return Ok(false);
            };
            let Some(input) = entry.input.take() else {
                bail!("native shell {shell_id} does not expose stdin EOF");
            };
            entry.record.capabilities.stdin_write = false;
            entry.record.capabilities.stdin_eof = false;
            entry.record.updated_at_ms = now_ms();
            persist_record(&entry.record, &entry.record_path)?;
            (input, entry.record.clone())
        };
        drop(input);
        self.publish_lifecycle(FerrousNativeLifecycleEventKind::Updated, updated_record);
        Ok(true)
    }

    pub fn send_shell_eof_blocking(&self, shell_id: &str) -> Result<FerrousShellInputResult> {
        let backend = self.live_shell_backend(shell_id)?;
        if !self.send_stdin_eof_blocking(shell_id)? {
            bail!("native shell {shell_id} not found or not live");
        }
        Ok(FerrousShellInputResult {
            shell_id: shell_id.to_owned(),
            backend,
            accepted: true,
            bytes_written: 0,
            newline_appended: false,
            eof_sent: true,
        })
    }

    pub fn resize_pty_blocking(&self, shell_id: &str, cols: u16, rows: u16) -> Result<bool> {
        let resize = {
            let state = self.lock_state()?;
            let Some(entry) = state.entries.get(shell_id) else {
                return Ok(false);
            };
            entry.resize.clone()
        };
        let Some(resize) = resize else {
            bail!("native shell {shell_id} does not expose PTY resize");
        };
        apply_pty_resize(resize.as_raw_fd(), cols, rows)?;
        Ok(true)
    }

    pub fn read_stdout_chunk_blocking(
        &self,
        shell_id: &str,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_blocking_output_available(shell_id)?;
        let output = self.output_for(shell_id)?;
        let Some(output) = output else {
            return Ok(None);
        };
        let mut output = output
            .lock()
            .map_err(|_| anyhow!("native output lock poisoned"))?;
        output.read_chunk(timeout)
    }

    pub fn read_stdout_available_blocking(
        &self,
        shell_id: &str,
        max_chunks: usize,
        timeout: Duration,
    ) -> Result<Vec<Vec<u8>>> {
        self.ensure_blocking_output_available(shell_id)?;
        let output = self.output_for(shell_id)?;
        let Some(output) = output else {
            return Ok(Vec::new());
        };
        let mut output = output
            .lock()
            .map_err(|_| anyhow!("native output lock poisoned"))?;
        output.read_available(max_chunks, timeout)
    }

    pub fn flush_stdout_log_blocking(&self, shell_id: &str) -> Result<bool> {
        let output = self.output_for(shell_id)?;
        let Some(output) = output else {
            return Ok(false);
        };
        let mut output = output
            .lock()
            .map_err(|_| anyhow!("native output lock poisoned"))?;
        output.flush_log_if_due(true)?;
        Ok(true)
    }

    pub fn read_line_blocking(&self, shell_id: &str, timeout: Duration) -> Result<Option<String>> {
        self.ensure_blocking_output_available(shell_id)?;
        let output = self.output_for(shell_id)?;
        let Some(output) = output else {
            return Ok(None);
        };
        let mut output = output
            .lock()
            .map_err(|_| anyhow!("native output lock poisoned"))?;
        output.read_line(timeout)
    }

    pub fn wait_shell_blocking(
        &self,
        shell_id: &str,
        timeout: Duration,
    ) -> Result<Option<FerrousNativeShellRecord>> {
        let start = Instant::now();
        loop {
            let Some(record) = self.get_shell(shell_id)? else {
                return Ok(None);
            };
            if record.status == FerrousNativeShellStatus::Exited {
                return Ok(Some(record));
            }
            if start.elapsed() >= timeout {
                return Ok(Some(record));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn shutdown_app_group_blocking(&self, app_id: &str) -> Result<FerrousShutdownResult> {
        let started_at_ms = now_ms() as u64;
        let targets = self
            .running_fws_records()?
            .into_iter()
            .filter(|record| record.app_id.as_deref() == Some(app_id))
            .collect::<Vec<_>>();
        let root_pids = targets
            .iter()
            .map(|record| i64::from(record.pid))
            .collect::<Vec<_>>();
        self.shutdown_process_tree_blocking(
            "shutdown_group",
            app_id,
            root_pids,
            "Ferrous native group shutdown terminates matching FWS roots and descendants",
            started_at_ms,
        )
    }

    pub fn shutdown_tree_blocking(&self, root_pids: Vec<i64>) -> Result<FerrousShutdownResult> {
        let started_at_ms = now_ms() as u64;
        let selected_roots = if root_pids.is_empty() {
            self.live_records()?
                .iter()
                .filter(|record| record.status == FerrousNativeShellStatus::Running)
                .filter(|record| pid_is_live_i64(i64::from(record.pid)))
                .map(|record| i64::from(record.pid))
                .collect::<Vec<_>>()
        } else {
            root_pids
        };
        let target = if selected_roots.is_empty() {
            "all".to_owned()
        } else {
            selected_roots
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        self.shutdown_process_tree_blocking(
            "shutdown_tree",
            &target,
            selected_roots,
            "Ferrous native tree shutdown terminates manager-owned roots and descendants",
            started_at_ms,
        )
    }

    pub fn shutdown_all_blocking(&self) -> Result<FerrousShutdownResult> {
        let mut result = self.shutdown_tree_blocking(Vec::new())?;
        result.kind = "shutdown_all".to_owned();
        result.target = "all".to_owned();
        result.note = Some(
            "Ferrous native all shutdown terminates manager-owned roots and descendants".to_owned(),
        );
        Ok(result)
    }

    fn shutdown_process_tree_blocking(
        &self,
        kind: &str,
        target: &str,
        root_pids: Vec<i64>,
        note: &str,
        started_at_ms: u64,
    ) -> Result<FerrousShutdownResult> {
        let running_records = self.running_fws_records()?;
        let root_pids = normalize_pids(root_pids);
        let snapshot = procfs_snapshot();
        let protected = protected_process_pids(&snapshot);
        let mut events = Vec::new();
        let plan = shutdown_plan_pids(&root_pids, &snapshot);
        let mut kill_plan = Vec::new();
        for pid in plan {
            if protected.contains(&pid) {
                events.push(format!("skipped protected pid {pid}"));
            } else {
                kill_plan.push(pid);
            }
        }
        let mut stats = FerrousShutdownStats {
            total: kill_plan.len() as u64,
            ..FerrousShutdownStats::default()
        };

        let mut term_sent = Vec::new();
        for pid in &kill_plan {
            if !pid_is_live_i64(*pid) {
                events.push(format!("pid {pid} already exited"));
                continue;
            }
            match signal_pid(*pid, libc::SIGTERM) {
                Ok(true) => {
                    stats.terminated += 1;
                    term_sent.push(*pid);
                    events.push(format!("sent SIGTERM to pid {pid}"));
                }
                Ok(false) => {
                    events.push(format!("pid {pid} exited before SIGTERM"));
                }
                Err(error) => {
                    stats.errors.push(format!("pid {pid}: {error}"));
                }
            }
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if term_sent.iter().all(|pid| !pid_is_live_i64(*pid)) {
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        let survivors = term_sent
            .iter()
            .copied()
            .filter(|pid| pid_is_live_i64(*pid))
            .collect::<Vec<_>>();
        stats.clean_exits = term_sent.len().saturating_sub(survivors.len()) as u64;

        for pid in &survivors {
            match signal_pid(*pid, libc::SIGKILL) {
                Ok(true) => {
                    stats.force_killed += 1;
                    events.push(format!("sent SIGKILL to pid {pid}"));
                }
                Ok(false) => {
                    events.push(format!("pid {pid} exited before SIGKILL"));
                }
                Err(error) => {
                    stats.errors.push(format!("pid {pid}: {error}"));
                }
            }
        }

        if !survivors.is_empty() {
            let kill_deadline = Instant::now() + Duration::from_secs(1);
            loop {
                if survivors.iter().all(|pid| !pid_is_live_i64(*pid)) {
                    break;
                }
                if Instant::now() >= kill_deadline {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }

        for record in running_records {
            let pid = i64::from(record.pid);
            if kill_plan.contains(&pid) && !pid_is_live_i64(pid) {
                let exit_code = if survivors.contains(&pid) {
                    Some(-libc::SIGKILL)
                } else {
                    Some(-libc::SIGTERM)
                };
                if let Err(error) = self.mark_record_exited(&record, exit_code) {
                    stats
                        .errors
                        .push(format!("{}: failed to mark exited: {error}", record.id));
                }
            }
        }

        let ended_at_ms = now_ms() as u64;
        Ok(FerrousShutdownResult {
            ok: stats.errors.is_empty(),
            kind: kind.to_owned(),
            target: target.to_owned(),
            started_at_ms,
            ended_at_ms,
            elapsed_ms: ended_at_ms.saturating_sub(started_at_ms),
            root_pids,
            stats,
            events,
            note: Some(note.to_owned()),
        })
    }

    fn running_fws_records(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        Ok(self
            .list_shells()?
            .into_iter()
            .filter(|record| {
                record.status == FerrousNativeShellStatus::Running
                    && pid_is_live_i64(i64::from(record.pid))
            })
            .collect())
    }

    fn mark_record_exited(
        &self,
        fallback: &FerrousNativeShellRecord,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let updated = {
            let mut state = self.lock_state()?;
            if let Some(entry) = state.entries.get_mut(&fallback.id) {
                entry.record.status = FerrousNativeShellStatus::Exited;
                entry.record.exit_code = exit_code;
                entry.record.updated_at_ms = now_ms();
                persist_record(&entry.record, &entry.record_path)?;
                Some(entry.record.clone())
            } else {
                None
            }
        };
        let updated = match updated {
            Some(record) => record,
            None => {
                let mut record = fallback.clone();
                record.status = FerrousNativeShellStatus::Exited;
                record.exit_code = exit_code;
                record.updated_at_ms = now_ms();
                persist_record(&record, &record.record_path)?;
                record
            }
        };
        self.publish_lifecycle(FerrousNativeLifecycleEventKind::Exited, updated);
        Ok(())
    }

    fn output_for(&self, shell_id: &str) -> Result<Option<Arc<Mutex<DirectOutput>>>> {
        let state = self.lock_state()?;
        let Some(entry) = state.entries.get(shell_id) else {
            return Ok(None);
        };
        Ok(entry.output.clone())
    }

    fn ensure_blocking_output_available(&self, shell_id: &str) -> Result<()> {
        let async_outputs = self
            .async_outputs
            .lock()
            .map_err(|_| anyhow!("native async output lock poisoned"))?;
        if async_outputs.contains_key(shell_id) {
            bail!("native shell {shell_id} stdout is owned by async reader");
        }
        Ok(())
    }

    fn async_stdout_output(&self, shell_id: &str) -> Result<Arc<AsyncDirectOutput>> {
        {
            let async_outputs = self
                .async_outputs
                .lock()
                .map_err(|_| anyhow!("native async output lock poisoned"))?;
            if let Some(output) = async_outputs.get(shell_id) {
                return Ok(Arc::clone(output));
            }
        }

        let (output, stdout_log) = {
            let state = self.lock_state()?;
            let Some(entry) = state.entries.get(shell_id) else {
                bail!("native shell {shell_id} not found or not live");
            };
            if entry.record.status != FerrousNativeShellStatus::Running {
                bail!("native shell {shell_id} is not running");
            }
            let Some(output) = entry.output.clone() else {
                bail!("native shell {shell_id} does not expose stdout output");
            };
            (output, entry.record.stdout_log.clone())
        };

        let owned_fd = {
            let mut output = output
                .lock()
                .map_err(|_| anyhow!("native output lock poisoned"))?;
            output.flush_log_if_due(true)?;
            duplicate_raw_fd(output.reader.as_raw_fd())?
        };
        set_nonblocking(&owned_fd)?;
        let reader = AsyncFd::new(owned_fd).context("failed to attach native stdout AsyncFd")?;
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stdout_log)
            .with_context(|| format!("failed to open stdout log {}", stdout_log.display()))?;
        let output = Arc::new(AsyncDirectOutput {
            shell_id: shell_id.to_owned(),
            stream: FerrousNativeOutputStream::Stdout,
            reader,
            log: AsyncMutex::new(AsyncOutputLog {
                log: BufWriter::with_capacity(256 * 1024, log_file),
                pending_flush_bytes: 0,
                last_flush_at: Instant::now(),
            }),
            subscriptions: Arc::clone(&self.subscriptions),
            subscriber_count: Arc::clone(&self.subscriber_count),
        });

        let mut async_outputs = self
            .async_outputs
            .lock()
            .map_err(|_| anyhow!("native async output lock poisoned"))?;
        Ok(Arc::clone(
            async_outputs
                .entry(shell_id.to_owned())
                .or_insert_with(|| output),
        ))
    }

    fn live_shell_backend(&self, shell_id: &str) -> Result<String> {
        let state = self.lock_state()?;
        let Some(entry) = state.entries.get(shell_id) else {
            bail!("native shell {shell_id} not found or not live");
        };
        if entry.record.status != FerrousNativeShellStatus::Running {
            bail!("native shell {shell_id} is not running");
        }
        Ok(entry.record.backend.clone())
    }

    fn live_running_record_by_spec_id(
        &self,
        spec_id: &str,
    ) -> Result<Option<FerrousNativeShellRecord>> {
        let state = self.lock_state()?;
        Ok(state
            .entries
            .values()
            .find(|entry| {
                entry.record.spec_id == spec_id
                    && entry.record.status == FerrousNativeShellStatus::Running
            })
            .map(|entry| entry.record.clone()))
    }

    fn insert_entry(
        &self,
        id: String,
        record: FerrousNativeShellRecord,
        record_path: PathBuf,
        child: Arc<Mutex<Child>>,
        input: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
        output: Option<Arc<Mutex<DirectOutput>>>,
        resize: Option<Arc<File>>,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        state.entries.insert(
            id,
            NativeShellEntry {
                record,
                record_path,
                child,
                input,
                output,
                resize,
            },
        );
        Ok(())
    }

    fn publish_lifecycle(
        &self,
        kind: FerrousNativeLifecycleEventKind,
        shell: FerrousNativeShellRecord,
    ) {
        let _ = self.lifecycle_tx.send(FerrousNativeLifecycleEvent {
            shell_id: shell.id.clone(),
            kind,
            shell,
        });
    }

    fn next_shell_id(&self) -> Result<String> {
        let mut state = self.lock_state()?;
        state.next_id += 1;
        let global_id = NEXT_GLOBAL_SHELL_ID.fetch_add(1, Ordering::Relaxed);
        Ok(format!(
            "frs_{}_{}_{}_{}",
            now_ms(),
            std::process::id(),
            global_id,
            state.next_id
        ))
    }

    fn merged_child_env(&self, explicit_env: HashMap<String, String>) -> HashMap<String, String> {
        let mut env = self.child_env_overlay();
        env.extend(explicit_env);
        env
    }

    fn resolve_log_dir(&self, log_dir: Option<PathBuf>) -> Result<PathBuf> {
        Ok(match log_dir {
            Some(path) => absolutize_path(expand_user(path)?)?,
            None => self.store.logs_dir.clone(),
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ManagerState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("native manager lock poisoned"))
    }
}

impl Default for FerrousNativeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FerrousFrameworkPipe {
    pub fn spawn(config: FerrousPipeConfig) -> Result<Self> {
        let manager = FerrousNativeManager::try_with_env_map(&config.env)?;
        let record = spawn_compat_pipe(&manager, config)?;
        wait_for_pipe_stdin_blocking(&manager, &record.id, Duration::from_secs(5))?;
        Ok(Self {
            manager,
            shell_id: record.id,
        })
    }

    pub fn shell_id(&self) -> Result<String> {
        Ok(self.shell_id.clone())
    }

    pub fn write_line_blocking(&self, line: &str) -> Result<()> {
        self.manager
            .write_line_strict_blocking(&self.shell_id, line)
    }

    pub fn read_line_blocking(&self) -> Result<Option<String>> {
        loop {
            if let Some(line) = self
                .manager
                .read_line_blocking(&self.shell_id, Duration::from_millis(250))?
            {
                return Ok(Some(line));
            }
            let Some(record) = self.manager.get_shell(&self.shell_id)? else {
                return Ok(None);
            };
            if record.status == FerrousNativeShellStatus::Exited {
                return Ok(None);
            }
        }
    }

    pub fn close_blocking(&self) -> Result<()> {
        self.manager
            .terminate_shell_strict_blocking(&self.shell_id, true)
    }
}

pub fn pyo3_embed_enabled() -> bool {
    true
}

pub fn ferrous_native_enabled() -> bool {
    true
}

impl FerrousNativeEnv {
    pub fn from_process_env() -> Self {
        Self::from_process_env_with_secret(read_env_or_else(
            FRAMEWORK_SHELLS_SECRET_KEY,
            generate_secret,
        ))
    }

    pub fn from_process_env_with_secret(secret: String) -> Self {
        Self {
            secret,
            run_id: read_env_or_else(FRAMEWORK_SHELLS_RUN_ID_KEY, generate_run_id),
            fws_socketio_url: nonempty_env(FRAMEWORK_SHELLS_FWS_SOCKETIO_URL_KEY),
            te_framework_url: nonempty_env(TE_FRAMEWORK_URL_KEY),
            extra: fws_child_extra_from_process_env(),
        }
    }

    pub fn from_env_map_with_secret(env_map: &HashMap<String, String>, secret: String) -> Self {
        Self {
            secret,
            run_id: env_map
                .get(FRAMEWORK_SHELLS_RUN_ID_KEY)
                .filter(|value| !value.is_empty())
                .cloned()
                .unwrap_or_else(|| read_env_or_else(FRAMEWORK_SHELLS_RUN_ID_KEY, generate_run_id)),
            fws_socketio_url: env_map
                .get(FRAMEWORK_SHELLS_FWS_SOCKETIO_URL_KEY)
                .filter(|value| !value.is_empty())
                .cloned()
                .or_else(|| nonempty_env(FRAMEWORK_SHELLS_FWS_SOCKETIO_URL_KEY)),
            te_framework_url: env_map
                .get(TE_FRAMEWORK_URL_KEY)
                .filter(|value| !value.is_empty())
                .cloned()
                .or_else(|| nonempty_env(TE_FRAMEWORK_URL_KEY)),
            extra: fws_child_extra_from_env_map(env_map),
        }
    }

    pub fn child_env_overlay(&self) -> HashMap<String, String> {
        let mut out = self.extra.clone();
        out.insert(FRAMEWORK_SHELLS_SECRET_KEY.to_owned(), self.secret.clone());
        out.insert(FRAMEWORK_SHELLS_RUN_ID_KEY.to_owned(), self.run_id.clone());
        if let Some(url) = &self.fws_socketio_url {
            out.insert(
                FRAMEWORK_SHELLS_FWS_SOCKETIO_URL_KEY.to_owned(),
                url.clone(),
            );
        }
        if let Some(url) = &self.te_framework_url {
            out.insert(TE_FRAMEWORK_URL_KEY.to_owned(), url.clone());
        }
        out.entry(FRAMEWORK_SHELLS_FWS_CHILD_KEY.to_owned())
            .or_insert_with(|| "1".to_owned());
        out
    }
}

impl FerrousNativeStore {
    pub fn from_process_env() -> Result<Self> {
        let base_dir = fws_base_dir()?;
        let repo_fingerprint = fws_repo_fingerprint()?;
        let secret = match nonempty_env(FRAMEWORK_SHELLS_SECRET_KEY) {
            Some(secret) => {
                persist_secret(&base_dir, &repo_fingerprint, &secret)?;
                secret
            }
            None => match load_stored_secret(&base_dir, &repo_fingerprint)? {
                Some(secret) => secret,
                None => {
                    let secret = generate_secret();
                    persist_secret(&base_dir, &repo_fingerprint, &secret)?;
                    secret
                }
            },
        };
        Self::from_base_fingerprint_secret(base_dir, repo_fingerprint, secret)
    }

    pub fn from_env_map(env_map: &HashMap<String, String>) -> Result<Self> {
        let base_dir = match env_map
            .get("FRAMEWORK_SHELLS_BASE_DIR")
            .filter(|value| !value.is_empty())
        {
            Some(raw) => absolutize_path(expand_user(PathBuf::from(raw))?)?,
            None => fws_base_dir()?,
        };
        let repo_fingerprint = match env_map
            .get("FRAMEWORK_SHELLS_REPO_FINGERPRINT")
            .filter(|value| !value.is_empty())
        {
            Some(value) => value.clone(),
            None => fws_repo_fingerprint()?,
        };
        let secret = match env_map
            .get(FRAMEWORK_SHELLS_SECRET_KEY)
            .filter(|value| !value.is_empty())
        {
            Some(secret) => {
                persist_secret(&base_dir, &repo_fingerprint, secret)?;
                secret.clone()
            }
            None => match load_stored_secret(&base_dir, &repo_fingerprint)? {
                Some(secret) => secret,
                None => {
                    let secret = generate_secret();
                    persist_secret(&base_dir, &repo_fingerprint, &secret)?;
                    secret
                }
            },
        };
        Self::from_base_fingerprint_secret(base_dir, repo_fingerprint, secret)
    }

    pub fn from_secret(secret: String) -> Result<Self> {
        let base_dir = fws_base_dir()?;
        let repo_fingerprint = fws_repo_fingerprint()?;
        persist_secret(&base_dir, &repo_fingerprint, &secret)?;
        Self::from_base_fingerprint_secret(base_dir, repo_fingerprint, secret)
    }

    pub fn from_base_dir_fingerprint_secret(
        base_dir: PathBuf,
        repo_fingerprint: String,
        secret: String,
    ) -> Result<Self> {
        let base_dir = absolutize_path(expand_user(base_dir)?)?;
        persist_secret(&base_dir, &repo_fingerprint, &secret)?;
        Self::from_base_fingerprint_secret(base_dir, repo_fingerprint, secret)
    }

    fn from_base_fingerprint_secret(
        base_dir: PathBuf,
        repo_fingerprint: String,
        secret: String,
    ) -> Result<Self> {
        let runtime_id = derive_runtime_id(&secret);
        let root = base_dir
            .join("runtimes")
            .join(&repo_fingerprint)
            .join(&runtime_id);
        let metadata_dir = root.join("meta");
        let logs_dir = root.join("logs");
        let sockets_dir = root.join("sockets");
        for dir in [&metadata_dir, &logs_dir, &sockets_dir] {
            create_dir_all(dir)
                .with_context(|| format!("failed to create FWS store dir {}", dir.display()))?;
        }
        let secret_file = base_dir
            .join("runtimes")
            .join(&repo_fingerprint)
            .join("secret");
        Ok(Self {
            secret,
            runtime_id,
            repo_fingerprint,
            base_dir,
            root,
            metadata_dir,
            logs_dir,
            sockets_dir,
            secret_file,
        })
    }
}

fn spawn_compat_pipe(
    manager: &FerrousNativeManager,
    config: FerrousPipeConfig,
) -> Result<FerrousNativeShellRecord> {
    let backend = config.backend.as_deref().unwrap_or("pipe");
    if backend != "pipe" {
        bail!("FerrousFrameworkPipe requires backend 'pipe', got '{backend}'");
    }
    if let Some(path) = &config.shellspec_path {
        let document = load_shellspec_document(path)?;
        let entry = choose_shellspec_entry(
            &document,
            config.shellspec_entry.as_deref(),
            config.spec_id.as_str(),
        )?;
        let input = compat_render_input(&config)?;
        let mut spec = render_shellspec_entry(&document, &entry, &input)?;
        if spec.backend != "pipe" {
            bail!(
                "FerrousFrameworkPipe shellspec entry '{}' rendered backend '{}'",
                spec.id,
                spec.backend
            );
        }
        if spec.cwd.is_none() {
            spec.cwd = config.cwd;
        }
        let label = nonempty_or(config.label, &spec.id);
        let spec_id = nonempty_or(config.spec_id, &spec.id);
        let subgroups = if config.subgroups.is_empty() {
            None
        } else {
            Some(config.subgroups)
        };
        return manager.spawn_rendered_shellspec_with_options_blocking(
            spec,
            config.log_dir,
            FerrousShellLaunchOverrides {
                env: config.env,
                label: Some(label),
                spec_id: Some(spec_id),
                subgroups,
                ui: None,
                debug: None,
                parent_shell_id: None,
            },
        );
    }
    manager.spawn_pipe_blocking(FerrousNativePipeConfig {
        command: config.command,
        cwd: config.cwd,
        env: config.env,
        label: nonempty_or(config.label, "ferrous-pipe"),
        spec_id: nonempty_or(config.spec_id, "ferrous-pipe"),
        subgroups: config.subgroups,
        log_dir: config.log_dir,
    })
}

fn load_shellspec_document(path: &Path) -> Result<Value> {
    let raw = read_to_string(path)
        .with_context(|| format!("failed to read shellspec {}", path.display()))?;
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse shellspec JSON {}", path.display()))
    } else {
        serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse shellspec YAML {}", path.display()))
    }
}

fn choose_shellspec_entry(
    document: &Value,
    requested: Option<&str>,
    spec_id: &str,
) -> Result<String> {
    let shells = document
        .get("shells")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("shellspec document missing shells object"))?;
    if let Some(entry) = requested.filter(|value| !value.is_empty()) {
        if shells.contains_key(entry) {
            return Ok(entry.to_owned());
        }
        bail!("shellspec id '{entry}' not found");
    }
    if !spec_id.is_empty() && shells.contains_key(spec_id) {
        return Ok(spec_id.to_owned());
    }
    shells
        .keys()
        .min()
        .cloned()
        .ok_or_else(|| anyhow!("shellspec document contains no shells"))
}

fn compat_render_input(config: &FerrousPipeConfig) -> Result<ShellspecRenderInput> {
    let mut ctx = config.ctx.clone();
    ctx.entry("PYTHON".to_owned())
        .or_insert_with(|| config.command.first().cloned().unwrap_or_default());
    let cwd = match &config.cwd {
        Some(path) => path_to_string(path),
        None => path_to_string(&env::current_dir().context("failed to read current directory")?),
    };
    ctx.entry("CWD".to_owned()).or_insert(cwd);
    Ok(ShellspecRenderInput {
        ctx,
        env: config.env.clone(),
    })
}

fn wait_for_pipe_stdin_blocking(
    manager: &FerrousNativeManager,
    shell_id: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(state) = manager.get_pipe_state(shell_id)? {
            if state.stdin_supported {
                return Ok(());
            }
            if state.status == FerrousNativeShellStatus::Exited {
                bail!("native pipe {shell_id} exited before stdin became ready");
            }
        }
        if Instant::now() >= deadline {
            bail!("native pipe {shell_id} stdin never became ready");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn nonempty_or(value: String, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value
    }
}

impl DirectOutput {
    fn read_line(&mut self, timeout: Duration) -> Result<Option<String>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(line) = take_line(&mut self.line_buffer) {
                return Ok(Some(line));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(now);
            let Some(chunk) = self.read_chunk(remaining)? else {
                return Ok(None);
            };
            self.line_buffer.extend_from_slice(&chunk);
        }
    }

    fn read_chunk(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let chunks = self.read_available(DIRECT_OUTPUT_MAX_DRAIN_CHUNKS, timeout)?;
        if chunks.is_empty() {
            return Ok(None);
        }
        let total_len = chunks.iter().map(Vec::len).sum();
        let mut out = Vec::with_capacity(total_len);
        for chunk in chunks {
            out.extend_from_slice(&chunk);
        }
        Ok(Some(out))
    }

    fn read_available(&mut self, max_chunks: usize, timeout: Duration) -> Result<Vec<Vec<u8>>> {
        let raw_fd = self.reader.as_raw_fd();
        // SAFETY: raw_fd is borrowed only for this poll call while self owns the fd.
        let borrowed = unsafe { BorrowedFd::borrow_raw(raw_fd) };
        let mut fds = [PollFd::new(
            borrowed,
            PollFlags::POLLIN | PollFlags::POLLHUP,
        )];
        let timeout = PollTimeout::try_from(timeout).unwrap_or(PollTimeout::MAX);
        let ready = poll(&mut fds, timeout).context("native output poll failed")?;
        if ready == 0 {
            self.flush_log_if_due(false)?;
            return Ok(Vec::new());
        }
        let mut chunks = Vec::new();
        let mut buffer = [0_u8; DIRECT_OUTPUT_READ_CHUNK_BYTES];
        for _ in 0..max_chunks.max(1) {
            match self.reader.read(&mut buffer) {
                Ok(0) => {
                    self.flush_log_if_due(true)?;
                    break;
                }
                Ok(n) => {
                    self.log.write_all(&buffer[..n])?;
                    self.pending_flush_bytes += n;
                    chunks.push(buffer[..n].to_vec());
                    self.flush_log_if_due(false)?;
                    if n < buffer.len() {
                        break;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }
        self.flush_log_if_due(false)?;
        if !chunks.is_empty() {
            broadcast_output_batch(
                &self.subscriptions,
                &self.subscriber_count,
                &self.shell_id,
                self.stream,
                &chunks,
            );
        }
        Ok(chunks)
    }

    fn flush_log_if_due(&mut self, force: bool) -> Result<()> {
        if self.pending_flush_bytes == 0 {
            return Ok(());
        }
        if force
            || self.pending_flush_bytes >= DIRECT_OUTPUT_LOG_FLUSH_BYTES
            || self.last_flush_at.elapsed()
                >= Duration::from_millis(DIRECT_OUTPUT_LOG_FLUSH_INTERVAL_MS)
        {
            self.log.flush()?;
            self.pending_flush_bytes = 0;
            self.last_flush_at = Instant::now();
        }
        Ok(())
    }
}

impl AsyncDirectOutput {
    async fn read_available(&self, max_chunks: usize) -> Result<Vec<Vec<u8>>> {
        loop {
            let mut guard = self
                .reader
                .readable()
                .await
                .context("native async stdout readiness failed")?;
            match guard.try_io(|inner| read_fd_available(inner.get_ref().as_raw_fd(), max_chunks)) {
                Ok(Ok(chunks)) => {
                    self.write_chunks_to_log(&chunks).await?;
                    if !chunks.is_empty() {
                        broadcast_output_batch(
                            &self.subscriptions,
                            &self.subscriber_count,
                            &self.shell_id,
                            self.stream,
                            &chunks,
                        );
                    }
                    return Ok(chunks);
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_would_block) => continue,
            }
        }
    }

    async fn write_chunks_to_log(&self, chunks: &[Vec<u8>]) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let mut log = self.log.lock().await;
        for chunk in chunks {
            log.log.write_all(chunk)?;
            log.pending_flush_bytes += chunk.len();
        }
        log.flush_if_due(false)
    }
}

impl AsyncOutputLog {
    fn flush_if_due(&mut self, force: bool) -> Result<()> {
        if self.pending_flush_bytes == 0 {
            return Ok(());
        }
        if force
            || self.pending_flush_bytes >= DIRECT_OUTPUT_LOG_FLUSH_BYTES
            || self.last_flush_at.elapsed()
                >= Duration::from_millis(DIRECT_OUTPUT_LOG_FLUSH_INTERVAL_MS)
        {
            self.log.flush()?;
            self.pending_flush_bytes = 0;
            self.last_flush_at = Instant::now();
        }
        Ok(())
    }
}

fn boxed_writer(writer: impl Write + Send + 'static) -> Arc<Mutex<Box<dyn Write + Send>>> {
    Arc::new(Mutex::new(Box::new(writer) as Box<dyn Write + Send>))
}

fn open_log(path: &PathBuf) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to create log {}", path.display()))
}

fn file_len(path: &PathBuf) -> u64 {
    metadata(path).map(|meta| meta.len()).unwrap_or_default()
}

fn record_path_for(metadata_dir: &std::path::Path, shell_id: &str) -> PathBuf {
    metadata_dir.join(shell_id).join("meta.json")
}

fn io_metadata_path_for(log_dir: &std::path::Path, shell_id: &str) -> PathBuf {
    log_dir.join(format!("{shell_id}.io_metadata.jsonl"))
}

fn derive_app_id(label: &str, subgroups: &[String]) -> Option<String> {
    label
        .strip_prefix("app-worker:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            subgroups
                .first()
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
}

fn derive_is_app_worker(label: &str, subgroups: &[String]) -> bool {
    label.starts_with("app-worker:")
        || subgroups
            .get(1)
            .map(String::as_str)
            .is_some_and(|value| value.trim() == "app-worker")
}

fn ms_to_seconds(value: u128) -> f64 {
    value as f64 / 1000.0
}

fn coerce_ms_timestamp(raw_ms: u128, raw_seconds: Option<f64>) -> u128 {
    if raw_ms != 0 {
        return raw_ms;
    }
    raw_seconds
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| (value * 1000.0) as u128)
        .unwrap_or_default()
}

fn default_true() -> bool {
    true
}

pub fn load_persisted_record(path: impl AsRef<Path>) -> Result<FerrousNativeShellRecord> {
    let path = path.as_ref();
    let raw = read_to_string(path)
        .with_context(|| format!("failed to read native record {}", path.display()))?;
    let persisted = parse_persisted_record_from_str(&raw)
        .with_context(|| format!("failed to parse native record {}", path.display()))?;
    let mut capabilities = persisted.capabilities;
    capabilities.stdin_write = false;
    capabilities.stdin_eof = false;
    capabilities.stdout_subscribe = false;
    capabilities.stderr_subscribe = false;
    capabilities.output_read = false;
    capabilities.terminate = false;
    capabilities.resize = false;
    let created_at_ms = coerce_ms_timestamp(persisted.created_at_ms, persisted.created_at);
    let updated_at_ms = coerce_ms_timestamp(persisted.updated_at_ms, persisted.updated_at);
    let id = persisted.id;
    let spec_id = persisted.spec_id.unwrap_or_else(|| id.clone());
    let backend = persisted
        .backend
        .unwrap_or_else(|| infer_backend_from_flags(persisted.uses_pty, persisted.uses_pipes));
    let label = persisted.label.unwrap_or_else(|| id.clone());
    let record_path = persisted
        .record_path
        .unwrap_or_else(|| path_to_string(path));
    let stdout_log = persisted.stdout_log.unwrap_or_else(|| {
        sibling_log_path(path, &id, "stdout.log").unwrap_or_else(|| format!("{id}.stdout.log"))
    });
    let stderr_log = persisted.stderr_log.unwrap_or_else(|| {
        sibling_log_path(path, &id, "stderr.log").unwrap_or_else(|| format!("{id}.stderr.log"))
    });
    let app_id = persisted
        .app_id
        .or_else(|| derive_app_id(&label, &persisted.subgroups));
    let is_app_worker =
        persisted.is_app_worker || derive_is_app_worker(&label, &persisted.subgroups);
    let pid = persisted.pid.unwrap_or_default();
    let mut status = persisted.status.unwrap_or(FerrousNativeShellStatus::Exited);
    if status == FerrousNativeShellStatus::Running && !pid_is_live(pid) {
        status = FerrousNativeShellStatus::Exited;
    }
    Ok(FerrousNativeShellRecord {
        id,
        backend,
        command: persisted.command,
        cwd: persisted.cwd.map(PathBuf::from),
        env: HashMap::new(),
        env_keys: persisted.env_keys,
        env_overrides: json_object_to_string_map(persisted.env_overrides),
        pid,
        status,
        exit_code: persisted.exit_code,
        label,
        spec_id,
        subgroups: persisted.subgroups,
        record_path: PathBuf::from(record_path),
        stdout_log: PathBuf::from(stdout_log),
        stderr_log: PathBuf::from(stderr_log),
        io_metadata_log: persisted.io_metadata_log.map(PathBuf::from),
        pty_mode: persisted.pty_mode,
        autostart: persisted.autostart,
        ui: persisted.ui,
        debug: persisted.debug,
        runtime_id: persisted.runtime_id,
        app_id,
        parent_shell_id: persisted.parent_shell_id,
        is_app_worker,
        capabilities,
        adopted: true,
        created_at_ms,
        updated_at_ms,
    })
}

fn pid_is_live(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    if Path::new("/proc").is_dir() {
        let Some(state) = linux_proc_pid_state(pid) else {
            return false;
        };
        return !matches!(state, 'Z' | 'X');
    }
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(code) if code == libc::EPERM
    )
}

fn pid_is_live_i64(pid: i64) -> bool {
    u32::try_from(pid).ok().is_some_and(pid_is_live)
}

fn linux_proc_pid_state(pid: u32) -> Option<char> {
    linux_proc_stat(pid).map(|entry| entry.state)
}

fn linux_proc_stat(pid: u32) -> Option<ProcfsEntry> {
    let stat = read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, after_comm) = stat.rsplit_once(") ")?;
    let mut fields = after_comm.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse::<i64>().ok()?;
    Some(ProcfsEntry {
        pid: i64::from(pid),
        ppid,
        state,
    })
}

fn procfs_snapshot() -> HashMap<i64, ProcfsEntry> {
    let mut processes = HashMap::new();
    if !Path::new("/proc").is_dir() {
        return processes;
    }
    let Ok(entries) = read_dir("/proc") else {
        return processes;
    };
    for entry in entries.flatten() {
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(pid) = file_name.parse::<u32>() else {
            continue;
        };
        let Some(process) = linux_proc_stat(pid) else {
            continue;
        };
        if matches!(process.state, 'Z' | 'X') {
            continue;
        }
        processes.insert(process.pid, process);
    }
    processes
}

fn protected_process_pids(snapshot: &HashMap<i64, ProcfsEntry>) -> HashSet<i64> {
    let mut protected = HashSet::new();
    let mut current = i64::from(std::process::id());
    while current > 0 && protected.insert(current) {
        let Some(entry) = snapshot.get(&current) else {
            break;
        };
        current = entry.ppid;
    }
    protected
}

fn normalize_pids(mut pids: Vec<i64>) -> Vec<i64> {
    pids.retain(|pid| *pid > 0);
    pids.sort_unstable();
    pids.dedup();
    pids
}

fn shutdown_plan_pids(root_pids: &[i64], snapshot: &HashMap<i64, ProcfsEntry>) -> Vec<i64> {
    let roots = root_pids.iter().copied().collect::<HashSet<_>>();
    let mut selected = roots.clone();
    loop {
        let mut changed = false;
        for process in snapshot.values() {
            if selected.contains(&process.ppid) && selected.insert(process.pid) {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let mut plan = selected.into_iter().collect::<Vec<_>>();
    plan.sort_by(|left, right| {
        process_depth(*right, snapshot, &roots)
            .cmp(&process_depth(*left, snapshot, &roots))
            .then_with(|| right.cmp(left))
    });
    plan
}

fn process_depth(pid: i64, snapshot: &HashMap<i64, ProcfsEntry>, roots: &HashSet<i64>) -> usize {
    let mut depth = 0;
    let mut current = pid;
    let mut seen = HashSet::new();
    while !roots.contains(&current) && seen.insert(current) {
        let Some(entry) = snapshot.get(&current) else {
            break;
        };
        current = entry.ppid;
        depth += 1;
    }
    depth
}

fn signal_pid(pid: i64, signal: libc::c_int) -> Result<bool> {
    if pid <= 0 || pid > i64::from(i32::MAX) {
        bail!("invalid pid {pid}");
    }
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error).with_context(|| format!("failed to signal pid {pid}"))
}

fn signal_pid_or_process_group(pid: i64, signal: libc::c_int) -> Result<bool> {
    if pid <= 0 || pid > i64::from(i32::MAX) {
        bail!("invalid pid {pid}");
    }
    let pid_t = pid as libc::pid_t;
    let pgid = unsafe { libc::getpgid(pid_t) };
    if pgid > 0 {
        let current_pgrp = unsafe { libc::getpgrp() };
        if pgid != current_pgrp {
            let result = unsafe { libc::killpg(pgid, signal) };
            if result == 0 {
                return Ok(true);
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error)
                    .with_context(|| format!("failed to signal process group {pgid}"));
            }
        }
    }
    signal_pid(pid, signal)
}

fn limit_exited_shell_history(
    records: Vec<FerrousNativeShellRecord>,
) -> Vec<FerrousNativeShellRecord> {
    let mut active = Vec::new();
    let mut exited = Vec::new();
    for record in records {
        if record.status == FerrousNativeShellStatus::Exited {
            exited.push(record);
        } else {
            active.push(record);
        }
    }
    exited.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
            .then_with(|| left.id.cmp(&right.id))
    });
    exited.truncate(MAX_EXITED_SHELLS);
    active.extend(exited);
    active.sort_by(|left, right| left.id.cmp(&right.id));
    active
}

fn parse_persisted_record_from_str(raw: &str) -> Result<PersistedNativeShellRecord> {
    let mut stream =
        serde_json::Deserializer::from_str(raw).into_iter::<PersistedNativeShellRecord>();
    match stream.next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(error)) => Err(error).context("failed to parse persisted shell record"),
        None => bail!("empty persisted shell record"),
    }
}

fn infer_backend_from_flags(uses_pty: bool, uses_pipes: bool) -> String {
    if uses_pipes {
        "pipe".to_owned()
    } else if uses_pty {
        "pty".to_owned()
    } else {
        "proc".to_owned()
    }
}

fn sibling_log_path(record_path: &Path, shell_id: &str, suffix: &str) -> Option<String> {
    let runtime_root = record_path.parent()?.parent()?.parent()?;
    Some(path_to_string(
        &runtime_root
            .join("logs")
            .join(format!("{shell_id}.{suffix}")),
    ))
}

fn list_persisted_records_in_dir(metadata_dir: &Path) -> Result<Vec<FerrousNativeShellRecord>> {
    if !metadata_dir.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in read_dir(metadata_dir).with_context(|| {
        format!(
            "failed to read native metadata dir {}",
            metadata_dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read native metadata dir entry {}",
                metadata_dir.display()
            )
        })?;
        let path = entry.path().join("meta.json");
        if !path.is_file() {
            continue;
        }
        match load_persisted_record(&path) {
            Ok(record) => records.push(record),
            Err(error) => eprintln!(
                "[ferrous-framework] skipping unreadable FWS record {}: {error:#}",
                path.display()
            ),
        }
    }
    records.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(records)
}

fn persist_record(record: &FerrousNativeShellRecord, path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)
            .with_context(|| format!("failed to create record dir {}", parent.display()))?;
    }
    let persisted = PersistedNativeShellRecord {
        id: record.id.clone(),
        spec_id: Some(record.spec_id.clone()),
        backend: Some(record.backend.clone()),
        command: record.command.clone(),
        cwd: record.cwd.as_ref().map(path_to_string),
        pid: Some(record.pid),
        status: Some(record.status.clone()),
        exit_code: record.exit_code,
        label: Some(record.label.clone()),
        subgroups: record.subgroups.clone(),
        record_path: Some(path_to_string(&record.record_path)),
        stdout_log: Some(path_to_string(&record.stdout_log)),
        stderr_log: Some(path_to_string(&record.stderr_log)),
        io_metadata_log: record.io_metadata_log.as_ref().map(path_to_string),
        pty_mode: record.pty_mode,
        autostart: record.autostart,
        ui: record.ui.clone(),
        debug: record.debug.clone(),
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        created_at: Some(ms_to_seconds(record.created_at_ms)),
        updated_at: Some(ms_to_seconds(record.updated_at_ms)),
        run_id: record.env.get("FRAMEWORK_SHELLS_RUN_ID").cloned(),
        launcher_pid: Some(std::process::id()),
        env_keys: record.env_keys.clone(),
        env_overrides: string_map_to_json_object(&record.env_overrides),
        uses_pty: record.backend == "pty",
        uses_pipes: record.backend == "pipe",
        uses_dtach: false,
        runtime_id: record.runtime_id.clone(),
        signature: None,
        app_id: record.app_id.clone(),
        parent_shell_id: record.parent_shell_id.clone(),
        is_app_worker: record.is_app_worker,
        capabilities: record.capabilities.clone(),
    };
    let tmp_path = path.with_extension("record.json.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create record {}", tmp_path.display()))?;
    serde_json::to_writer_pretty(file, &persisted)
        .with_context(|| format!("failed to write record {}", tmp_path.display()))?;
    rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to install record {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn path_to_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().into_owned()
}

fn sorted_env_keys(env: &HashMap<String, String>) -> Vec<String> {
    let mut keys = env.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys
}

fn string_map_to_json_object(values: &HashMap<String, String>) -> Map<String, Value> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect()
}

fn json_object_to_string_map(values: Map<String, Value>) -> HashMap<String, String> {
    values
        .into_iter()
        .filter_map(|(key, value)| match value {
            Value::String(value) => Some((key, value)),
            Value::Number(value) => Some((key, value.to_string())),
            Value::Bool(value) => Some((key, value.to_string())),
            Value::Null => Some((key, String::new())),
            Value::Array(_) | Value::Object(_) => None,
        })
        .collect()
}

fn set_nonblocking(fd: &impl AsFd) -> Result<()> {
    let flags = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

fn duplicate_raw_fd(raw_fd: i32) -> Result<OwnedFd> {
    let duplicated = unsafe { libc::dup(raw_fd) };
    if duplicated < 0 {
        return Err(IoError::last_os_error()).context("failed to duplicate native stdout fd");
    }
    // SAFETY: `dup` returns a fresh owned file descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

fn read_fd_available(raw_fd: i32, max_chunks: usize) -> std::io::Result<Vec<Vec<u8>>> {
    let mut chunks = Vec::new();
    let mut buffer = [0_u8; DIRECT_OUTPUT_READ_CHUNK_BYTES];
    for _ in 0..max_chunks.max(1) {
        let read_count = unsafe {
            libc::read(
                raw_fd,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };
        if read_count == 0 {
            return Ok(chunks);
        }
        if read_count > 0 {
            let read_count = usize::try_from(read_count).unwrap_or_default();
            chunks.push(buffer[..read_count].to_vec());
            if read_count < buffer.len() {
                return Ok(chunks);
            }
            continue;
        }

        let error = IoError::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == ErrorKind::WouldBlock {
            if chunks.is_empty() {
                return Err(error);
            }
            return Ok(chunks);
        }
        return Err(error);
    }
    Ok(chunks)
}

fn parse_pty_mode(raw: &str) -> Result<FerrousNativePtyMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "interactive" | "cooked" => Ok(FerrousNativePtyMode::Interactive),
        "raw" => Ok(FerrousNativePtyMode::Raw),
        other => bail!("unsupported native PTY mode '{other}'"),
    }
}

fn apply_pty_mode(fd: i32, mode: FerrousNativePtyMode) -> Result<()> {
    match mode {
        FerrousNativePtyMode::Interactive => Ok(()),
        FerrousNativePtyMode::Raw => apply_pty_raw_mode(fd),
    }
}

fn apply_pty_raw_mode(fd: i32) -> Result<()> {
    // SAFETY: zeroed termios is immediately initialized by tcgetattr before use.
    let mut termios = unsafe { std::mem::zeroed::<libc::termios>() };
    // SAFETY: fd is the live PTY slave fd, and termios points to valid writable storage.
    if unsafe { libc::tcgetattr(fd, &mut termios) } == -1 {
        return Err(std::io::Error::last_os_error()).context("failed to read PTY termios");
    }
    // SAFETY: cfmakeraw mutates a valid termios struct in place.
    unsafe {
        libc::cfmakeraw(&mut termios);
    }
    // SAFETY: fd is the live PTY slave fd, and termios points to initialized settings.
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &termios) } == -1 {
        return Err(std::io::Error::last_os_error()).context("failed to set PTY raw mode");
    }
    Ok(())
}

fn apply_pty_resize(fd: i32, cols: u16, rows: u16) -> Result<()> {
    let size = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: fd is a live PTY master fd owned by the native shell entry, and
    // the pointer references a stack-allocated winsize for the duration of the
    // ioctl call.
    let result = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &size) };
    if result == -1 {
        return Err(std::io::Error::last_os_error()).context("failed to resize native PTY");
    }
    Ok(())
}

fn validate_command(command: &[String], backend: &str) -> Result<()> {
    if command.is_empty() {
        bail!("{backend} command cannot be empty");
    }
    Ok(())
}

fn wait_for_readiness_blocking(
    record: &FerrousNativeShellRecord,
    probe: &RenderedReadinessProbe,
) -> Result<bool> {
    match probe.probe_type.as_str() {
        "tcp_port" => wait_for_tcp_port(probe),
        "stdout_regex" => wait_for_stdout_regex(record, probe),
        other => bail!("unsupported readiness probe '{other}'"),
    }
}

fn wait_for_tcp_port(probe: &RenderedReadinessProbe) -> Result<bool> {
    let Some(port) = probe.port else {
        return Ok(false);
    };
    let deadline = Instant::now() + readiness_timeout(probe);
    let address = format!("{}:{port}", probe.host);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = remaining.min(Duration::from_millis(200));
        let addrs = address
            .to_socket_addrs()
            .with_context(|| format!("failed to resolve readiness address {address}"))?
            .collect::<Vec<_>>();
        for addr in addrs {
            if TcpStream::connect_timeout(&addr, connect_timeout).is_ok() {
                return Ok(true);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(false)
}

fn wait_for_stdout_regex(
    record: &FerrousNativeShellRecord,
    probe: &RenderedReadinessProbe,
) -> Result<bool> {
    let Some(pattern) = &probe.pattern else {
        return Ok(false);
    };
    let regex =
        Regex::new(pattern).with_context(|| format!("invalid readiness regex {pattern:?}"))?;
    let deadline = Instant::now() + readiness_timeout(probe);
    while Instant::now() < deadline {
        match read_to_string(&record.stdout_log) {
            Ok(content) if regex.is_match(&content) => return Ok(true),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(false)
}

fn readiness_timeout(probe: &RenderedReadinessProbe) -> Duration {
    if probe.timeout_seconds.is_finite() && probe.timeout_seconds > 0.0 {
        Duration::from_secs_f64(probe.timeout_seconds)
    } else {
        Duration::from_secs(30)
    }
}

fn fws_base_dir() -> Result<PathBuf> {
    let base = match nonempty_env("FRAMEWORK_SHELLS_BASE_DIR") {
        Some(raw) => expand_user(PathBuf::from(raw))?,
        None => home_dir()?.join(".cache").join("framework_shells"),
    };
    absolutize_path(base)
}

fn fws_repo_fingerprint() -> Result<String> {
    if let Some(fingerprint) = nonempty_env("FRAMEWORK_SHELLS_REPO_FINGERPRINT") {
        return Ok(fingerprint);
    }
    let fingerprint = if truthy_env("FRAMEWORK_SHELLS_ALLOW_NO_FINGERPRINT") {
        "standalone_debug".to_owned()
    } else {
        compute_fingerprint_from_cwd()?
    };
    // SAFETY: This mirrors Python FWS bootstrap by exporting the computed
    // fingerprint for later code in the same runtime. Ferrous does this during
    // manager initialization, before it spawns child worker threads.
    unsafe {
        env::set_var("FRAMEWORK_SHELLS_REPO_FINGERPRINT", &fingerprint);
    }
    Ok(fingerprint)
}

fn compute_fingerprint_from_cwd() -> Result<String> {
    let cwd = env::current_dir().context("failed to read current directory")?;
    let cwd = absolutize_path(cwd)?;
    Ok(sha256_hex(cwd.to_string_lossy().as_bytes())[..16].to_owned())
}

fn load_stored_secret(
    base_dir: &std::path::Path,
    repo_fingerprint: &str,
) -> Result<Option<String>> {
    let secret_file = secret_file_path(base_dir, repo_fingerprint);
    if !secret_file.exists() {
        return Ok(None);
    }
    let secret = read_to_string(&secret_file)
        .with_context(|| format!("failed to read FWS secret {}", secret_file.display()))?
        .trim()
        .to_owned();
    Ok((!secret.is_empty()).then_some(secret))
}

fn persist_secret(base_dir: &std::path::Path, repo_fingerprint: &str, secret: &str) -> Result<()> {
    if secret.is_empty() {
        return Ok(());
    }
    let secret_file = secret_file_path(base_dir, repo_fingerprint);
    if let Some(parent) = secret_file.parent() {
        create_dir_all(parent)
            .with_context(|| format!("failed to create FWS secret dir {}", parent.display()))?;
    }
    std::fs::write(&secret_file, secret)
        .with_context(|| format!("failed to write FWS secret {}", secret_file.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&secret_file, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn secret_file_path(base_dir: &std::path::Path, repo_fingerprint: &str) -> PathBuf {
    base_dir
        .join("runtimes")
        .join(repo_fingerprint)
        .join("secret")
}

fn derive_runtime_id(secret: &str) -> String {
    sha256_hex(secret.as_bytes())[..16].to_owned()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_bytes(&digest)
}

fn home_dir() -> Result<PathBuf> {
    env::var("HOME")
        .map(PathBuf::from)
        .context("HOME is required to resolve FWS base dir")
}

fn expand_user(path: PathBuf) -> Result<PathBuf> {
    let raw = path.to_string_lossy();
    if raw == "~" {
        return home_dir();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return Ok(home_dir()?.join(rest));
    }
    Ok(path)
}

fn absolutize_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(env::current_dir()
        .context("failed to read current directory")?
        .join(path))
}

fn truthy_env(name: &str) -> bool {
    let Some(raw) = nonempty_env(name) else {
        return false;
    };
    truthy_value(&raw)
}

fn truthy_value(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

fn native_env_requests_parent_peer(native_env: &FerrousNativeEnv) -> bool {
    env_flag_truthy(&native_env.extra, FRAMEWORK_SHELLS_FWS_CHILD_KEY)
        || env_flag_truthy(&native_env.extra, LEGACY_FWS_CHILD_KEY)
}

fn native_env_disables_parent_peer(native_env: &FerrousNativeEnv) -> bool {
    env_flag_truthy(
        &native_env.extra,
        FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY,
    )
}

fn env_flag_truthy(env: &HashMap<String, String>, name: &str) -> bool {
    env.get(name).is_some_and(|value| truthy_value(value))
}

fn fws_child_extra_from_process_env() -> HashMap<String, String> {
    let mut extra = HashMap::new();
    if truthy_env(FRAMEWORK_SHELLS_FWS_CHILD_KEY) || truthy_env(LEGACY_FWS_CHILD_KEY) {
        extra.insert(FRAMEWORK_SHELLS_FWS_CHILD_KEY.to_owned(), "1".to_owned());
    }
    if truthy_env(FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY) {
        extra.insert(
            FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY.to_owned(),
            "1".to_owned(),
        );
    }
    extra
}

fn fws_child_extra_from_env_map(env_map: &HashMap<String, String>) -> HashMap<String, String> {
    let mut extra = fws_child_extra_from_process_env();
    if env_flag_truthy(env_map, FRAMEWORK_SHELLS_FWS_CHILD_KEY)
        || env_flag_truthy(env_map, LEGACY_FWS_CHILD_KEY)
    {
        extra.insert(FRAMEWORK_SHELLS_FWS_CHILD_KEY.to_owned(), "1".to_owned());
    }
    if env_flag_truthy(env_map, FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY) {
        extra.insert(
            FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER_KEY.to_owned(),
            "1".to_owned(),
        );
    }
    extra
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .and_then(|value| (!value.is_empty()).then_some(value))
}

fn read_env_or_else(name: &str, fallback: impl FnOnce() -> String) -> String {
    nonempty_env(name).unwrap_or_else(fallback)
}

fn generate_secret() -> String {
    let mut bytes = [0_u8; 8];
    if getrandom::fill(&mut bytes).is_ok() {
        return format!("temporary_secret_{}", hex_bytes(&bytes));
    }
    format!("temporary_secret_{}_{}", std::process::id(), now_ms())
}

fn generate_run_id() -> String {
    format!("ferrous_run_{}_{}", now_ms(), std::process::id())
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn take_line(buffer: &mut Vec<u8>) -> Option<String> {
    let newline = buffer.iter().position(|byte| *byte == b'\n')?;
    let mut raw = buffer.drain(..=newline).collect::<Vec<_>>();
    if raw.ends_with(b"\n") {
        raw.pop();
    }
    if raw.ends_with(b"\r") {
        raw.pop();
    }
    Some(String::from_utf8_lossy(&raw).into_owned())
}

fn reactor_loop(
    state: Arc<Mutex<ManagerState>>,
    subscriptions: Arc<Mutex<OutputSubscriptions>>,
    lifecycle_tx: broadcast::Sender<FerrousNativeLifecycleEvent>,
    rx: Receiver<ReactorCommand>,
) {
    let mut streams = Vec::<ReactorLogStream>::new();
    let mut children = Vec::<ReactorChild>::new();
    loop {
        if streams.is_empty() {
            let received = if children.is_empty() {
                rx.recv().ok()
            } else {
                match rx.recv_timeout(Duration::from_millis(25)) {
                    Ok(command) => Some(command),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                }
            };
            if let Some(command) = received {
                handle_reactor_command(command, &mut streams, &mut children);
                drain_reactor_commands(&rx, &mut streams, &mut children);
            }
            check_reactor_children(&state, &lifecycle_tx, &mut children);
            continue;
        }

        drain_reactor_commands(&rx, &mut streams, &mut children);
        poll_reactor_streams(&mut streams, &subscriptions, Duration::from_millis(25));
        check_reactor_children(&state, &lifecycle_tx, &mut children);
    }
}

fn drain_reactor_commands(
    rx: &Receiver<ReactorCommand>,
    streams: &mut Vec<ReactorLogStream>,
    children: &mut Vec<ReactorChild>,
) {
    loop {
        match rx.try_recv() {
            Ok(command) => handle_reactor_command(command, streams, children),
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Disconnected) => return,
        }
    }
}

fn handle_reactor_command(
    command: ReactorCommand,
    streams: &mut Vec<ReactorLogStream>,
    children: &mut Vec<ReactorChild>,
) {
    match command {
        ReactorCommand::RegisterLogStream(stream) => streams.push(stream),
        ReactorCommand::WatchChild(child) => children.push(child),
    }
}

fn poll_reactor_streams(
    streams: &mut Vec<ReactorLogStream>,
    subscriptions: &Arc<Mutex<OutputSubscriptions>>,
    timeout: Duration,
) {
    let mut fds = streams
        .iter()
        .map(|stream| {
            let raw_fd = stream.reader.as_raw_fd();
            // SAFETY: raw_fd remains owned by `streams` for the duration of this
            // poll call; fds are dropped before any stream is mutated.
            let borrowed = unsafe { BorrowedFd::borrow_raw(raw_fd) };
            PollFd::new(borrowed, PollFlags::POLLIN | PollFlags::POLLHUP)
        })
        .collect::<Vec<_>>();
    let timeout = PollTimeout::try_from(timeout).unwrap_or(PollTimeout::MAX);
    let Ok(ready_count) = poll(&mut fds, timeout) else {
        return;
    };
    if ready_count == 0 {
        return;
    }
    let ready_indices = fds
        .iter()
        .enumerate()
        .filter_map(|(index, fd)| {
            fd.revents()
                .is_some_and(|events| events.intersects(PollFlags::POLLIN | PollFlags::POLLHUP))
                .then_some(index)
        })
        .collect::<Vec<_>>();
    drop(fds);

    for index in ready_indices.into_iter().rev() {
        if !drain_reactor_stream(&mut streams[index], subscriptions) {
            streams.swap_remove(index);
        }
    }
}

fn drain_reactor_stream(
    stream: &mut ReactorLogStream,
    subscriptions: &Arc<Mutex<OutputSubscriptions>>,
) -> bool {
    let mut buffer = [0_u8; 64 * 1024];
    let mut wrote = false;
    loop {
        match stream.reader.read(&mut buffer) {
            Ok(0) => {
                let _ = stream.log.flush();
                return false;
            }
            Ok(n) => {
                if stream.log.write_all(&buffer[..n]).is_err() {
                    return false;
                }
                broadcast_output(
                    subscriptions,
                    &stream.subscriber_count,
                    &stream.shell_id,
                    stream.stream,
                    &buffer[..n],
                );
                wrote = true;
                if n < buffer.len() {
                    break;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => return false,
        }
    }
    if wrote && stream.log.flush().is_err() {
        return false;
    }
    true
}

fn broadcast_output(
    subscriptions: &Arc<Mutex<OutputSubscriptions>>,
    subscriber_count: &Arc<AtomicU64>,
    shell_id: &str,
    stream: FerrousNativeOutputStream,
    bytes: &[u8],
) {
    if bytes.is_empty() || subscriber_count.load(Ordering::Relaxed) == 0 {
        return;
    }
    broadcast_output_batch(
        subscriptions,
        subscriber_count,
        shell_id,
        stream,
        &[bytes.to_vec()],
    );
}

fn broadcast_output_batch(
    subscriptions: &Arc<Mutex<OutputSubscriptions>>,
    subscriber_count: &Arc<AtomicU64>,
    shell_id: &str,
    stream: FerrousNativeOutputStream,
    chunks: &[Vec<u8>],
) {
    if chunks.is_empty() || subscriber_count.load(Ordering::Relaxed) == 0 {
        return;
    }
    let Ok(mut subscriptions) = subscriptions.lock() else {
        return;
    };
    let key = (shell_id.to_owned(), stream);
    let Some(mut subscribers) = subscriptions.subscribers.remove(&key) else {
        return;
    };
    let mut dropped_count = subscriptions.dropped.get(&key).copied().unwrap_or(0);
    let mut retained = Vec::with_capacity(subscribers.len());
    let mut removed_subscribers = 0_u64;
    for subscriber in subscribers.drain(..) {
        let mut keep = true;
        for bytes in chunks {
            let chunk = FerrousNativeOutputChunk {
                shell_id: shell_id.to_owned(),
                stream,
                bytes: bytes.clone(),
                dropped_before: dropped_count,
            };
            match subscriber.tx.try_send(chunk) {
                Ok(()) => {}
                Err(CrossbeamTrySendError::Full(_)) => {
                    dropped_count += 1;
                    keep = false;
                    break;
                }
                Err(CrossbeamTrySendError::Disconnected(_)) => {
                    keep = false;
                    break;
                }
            }
        }
        if keep {
            retained.push(subscriber);
        } else {
            removed_subscribers += 1;
        }
    }
    if !retained.is_empty() {
        subscriptions.subscribers.insert(key.clone(), retained);
    }
    subscriptions.dropped.insert(key, dropped_count);
    if removed_subscribers > 0 {
        decrement_subscriber_count(subscriber_count, removed_subscribers);
    }
}

fn decrement_subscriber_count(subscriber_count: &Arc<AtomicU64>, amount: u64) {
    let _ = subscriber_count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

fn check_reactor_children(
    state: &Arc<Mutex<ManagerState>>,
    lifecycle_tx: &broadcast::Sender<FerrousNativeLifecycleEvent>,
    children: &mut Vec<ReactorChild>,
) {
    let mut exited = Vec::<(usize, i32)>::new();
    for (index, child_entry) in children.iter().enumerate() {
        let exit_code = {
            let Ok(mut child) = child_entry.child.lock() else {
                exited.push((index, -1));
                continue;
            };
            match child.try_wait() {
                Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
                Ok(None) => None,
                Err(_) => Some(-1),
            }
        };
        if let Some(exit_code) = exit_code {
            exited.push((index, exit_code));
        }
    }
    for (index, exit_code) in exited.into_iter().rev() {
        let child_entry = children.swap_remove(index);
        let mut updated_record = None;
        if let Ok(mut state) = state.lock() {
            if let Some(entry) = state.entries.get_mut(&child_entry.shell_id) {
                entry.record.status = FerrousNativeShellStatus::Exited;
                entry.record.exit_code = Some(exit_code);
                entry.record.updated_at_ms = now_ms();
                let _ = persist_record(&entry.record, &entry.record_path);
                updated_record = Some(entry.record.clone());
            }
        }
        if let Some(shell) = updated_record {
            let _ = lifecycle_tx.send(FerrousNativeLifecycleEvent {
                shell_id: shell.id.clone(),
                kind: FerrousNativeLifecycleEventKind::Exited,
                shell,
            });
        }
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
