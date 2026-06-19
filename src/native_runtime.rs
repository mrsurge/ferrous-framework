use crate::shellspec::{
    RenderedReadinessProbe, RenderedShellSpec, ShellspecRenderInput, render_shellspec_entries,
    render_shellspec_entry,
};
use anyhow::{Context, Result, anyhow, bail};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    poll::{PollFd, PollFlags, PollTimeout, poll},
    pty::openpty,
    unistd::dup,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    fs::{File, OpenOptions, create_dir_all, read_dir, read_to_string, rename},
    io::{BufWriter, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    os::fd::{AsFd, AsRawFd, BorrowedFd},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, TryRecvError, channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeShellRecord {
    pub id: String,
    pub backend: String,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub env_keys: Vec<String>,
    pub pid: u32,
    pub status: FerrousNativeShellStatus,
    pub exit_code: Option<i32>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub record_path: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
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
}

#[derive(Clone)]
pub struct FerrousNativeManager {
    state: Arc<Mutex<ManagerState>>,
    reactor: NativeRuntimeReactor,
    store: FerrousNativeStore,
    native_env: FerrousNativeEnv,
}

#[derive(Default)]
struct ManagerState {
    next_id: u64,
    entries: HashMap<String, NativeShellEntry>,
}

struct NativeShellEntry {
    record: FerrousNativeShellRecord,
    record_path: PathBuf,
    child: Arc<Mutex<Child>>,
    input: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    output: Option<Arc<Mutex<DirectOutput>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedNativeShellRecord {
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
    created_at_ms: u128,
    updated_at_ms: u128,
    run_id: Option<String>,
    launcher_pid: u32,
    env_keys: Vec<String>,
    capabilities: FerrousNativeShellCapabilities,
}

struct DirectOutput {
    reader: Box<dyn ReadAsFd>,
    log: BufWriter<File>,
    line_buffer: Vec<u8>,
}

trait ReadAsFd: Read + AsRawFd + Send {}
impl<T: Read + AsRawFd + Send> ReadAsFd for T {}

#[derive(Clone)]
struct NativeRuntimeReactor {
    tx: Sender<ReactorCommand>,
}

enum ReactorCommand {
    RegisterLogStream(ReactorLogStream),
    WatchChild(ReactorChild),
}

struct ReactorLogStream {
    reader: Box<dyn ReadAsFd>,
    log: BufWriter<File>,
}

struct ReactorChild {
    shell_id: String,
    child: Arc<Mutex<Child>>,
}

impl NativeRuntimeReactor {
    fn start(state: Arc<Mutex<ManagerState>>) -> Self {
        let (tx, rx) = channel::<ReactorCommand>();
        thread::spawn(move || reactor_loop(state, rx));
        Self { tx }
    }

    fn register_log_stream(&self, reader: impl ReadAsFd + 'static, file: File) -> Result<()> {
        self.tx
            .send(ReactorCommand::RegisterLogStream(ReactorLogStream {
                reader: Box::new(reader),
                log: BufWriter::with_capacity(256 * 1024, file),
            }))
            .map_err(|_| anyhow!("native runtime reactor stopped"))
    }

    fn watch_child(&self, shell_id: String, child: Arc<Mutex<Child>>) -> Result<()> {
        self.tx
            .send(ReactorCommand::WatchChild(ReactorChild { shell_id, child }))
            .map_err(|_| anyhow!("native runtime reactor stopped"))
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

    pub fn with_store_and_env(store: FerrousNativeStore, native_env: FerrousNativeEnv) -> Self {
        let state = Arc::new(Mutex::new(ManagerState::default()));
        let reactor = NativeRuntimeReactor::start(Arc::clone(&state));
        Self {
            state,
            reactor,
            store,
            native_env,
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

    pub fn spawn_shellspec_entry_blocking(
        &self,
        document: &Value,
        entry: &str,
        input: &ShellspecRenderInput,
    ) -> Result<FerrousNativeShellRecord> {
        let spec = render_shellspec_entry(document, entry, input)?;
        self.spawn_rendered_shellspec_blocking(spec)
    }

    pub fn spawn_rendered_shellspec_blocking(
        &self,
        spec: RenderedShellSpec,
    ) -> Result<FerrousNativeShellRecord> {
        if !spec.autostart {
            bail!("shellspec '{}' has autostart=false", spec.id);
        }
        let readiness = spec.readiness.clone();
        let record = match spec.backend.as_str() {
            "proc" => self.spawn_proc_blocking(FerrousNativeProcConfig {
                command: spec.command,
                cwd: spec.cwd,
                env: spec.env,
                label: spec.id.clone(),
                spec_id: spec.id,
                subgroups: spec.subgroups,
                log_dir: None,
            }),
            "pipe" => self.spawn_pipe_blocking(FerrousNativePipeConfig {
                command: spec.command,
                cwd: spec.cwd,
                env: spec.env,
                label: spec.id.clone(),
                spec_id: spec.id,
                subgroups: spec.subgroups,
                log_dir: None,
            }),
            "pty" => self.spawn_pty_blocking(FerrousNativePtyConfig {
                command: spec.command,
                cwd: spec.cwd,
                env: spec.env,
                label: spec.id.clone(),
                spec_id: spec.id,
                subgroups: spec.subgroups,
                log_dir: None,
            }),
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
        let record_path = record_path_for(&log_dir, &id);
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
            self.reactor.register_log_stream(stdout, stdout_file)?;
        }
        if let Some(stderr) = child.stderr.take() {
            set_nonblocking(&stderr)?;
            self.reactor.register_log_stream(stderr, stderr_file)?;
        }

        let now = now_ms();
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "proc".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: false,
                stdin_eof: false,
                stdout_log: true,
                stderr_log: true,
                stdout_subscribe: false,
                stderr_subscribe: false,
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
        )?;
        self.reactor.watch_child(id, child)?;
        Ok(record)
    }

    pub fn spawn_pipe_blocking(
        &self,
        config: FerrousNativePipeConfig,
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
        let record_path = record_path_for(&log_dir, &id);
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
            reader: Box::new(stdout),
            log: BufWriter::with_capacity(256 * 1024, stdout_file),
            line_buffer: Vec::new(),
        })));
        if let Some(stderr) = child.stderr.take() {
            set_nonblocking(&stderr)?;
            self.reactor.register_log_stream(stderr, stderr_file)?;
        }

        let now = now_ms();
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "pipe".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: input.is_some(),
                stdin_eof: input.is_some(),
                stdout_log: true,
                stderr_log: true,
                stdout_subscribe: false,
                stderr_subscribe: false,
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
        )?;
        self.reactor.watch_child(id, child)?;
        Ok(record)
    }

    pub fn spawn_pty_blocking(
        &self,
        config: FerrousNativePtyConfig,
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
        let record_path = record_path_for(&log_dir, &id);
        let stdout_file = open_log(&stdout_log)?;
        File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;

        let pty = openpty(None, None).context("failed to open PTY")?;
        let slave_stdin = dup(&pty.slave).context("failed to duplicate PTY slave stdin")?;
        let slave_stdout = dup(&pty.slave).context("failed to duplicate PTY slave stdout")?;
        let slave_stderr = pty.slave;
        let master_input = dup(&pty.master).context("failed to duplicate PTY master input")?;
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
            reader: Box::new(master_output),
            log: BufWriter::with_capacity(256 * 1024, stdout_file),
            line_buffer: Vec::new(),
        })));

        let now = now_ms();
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "pty".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env_keys: sorted_env_keys(&child_env),
            env: child_env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            record_path: record_path.clone(),
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: true,
                stdin_eof: true,
                stdout_log: true,
                stderr_log: false,
                stdout_subscribe: false,
                stderr_subscribe: false,
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
        )?;
        self.reactor.watch_child(id, child)?;
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
        Ok(records.into_values().collect())
    }

    pub fn get_shell(&self, shell_id: &str) -> Result<Option<FerrousNativeShellRecord>> {
        {
            let state = self.lock_state()?;
            if let Some(entry) = state.entries.get(shell_id) {
                return Ok(Some(entry.record.clone()));
            }
        }
        let record_path = record_path_for(&self.store.logs_dir, shell_id);
        if record_path.exists() {
            return Ok(Some(load_persisted_record(&record_path)?));
        }
        Ok(None)
    }

    pub fn list_persisted_records(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        list_persisted_records_in_dir(&self.store.logs_dir)
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
            let Some(entry) = state.entries.get(shell_id) else {
                return Ok(false);
            };
            if entry.record.status == FerrousNativeShellStatus::Exited {
                return Ok(true);
            }
            Arc::clone(&entry.child)
        };
        let mut child = child
            .lock()
            .map_err(|_| anyhow!("native child lock poisoned"))?;
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        Ok(true)
    }

    pub fn write_line_blocking(&self, shell_id: &str, line: &str) -> Result<bool> {
        self.write_blocking(shell_id, line.as_bytes())?;
        self.write_blocking(shell_id, b"\n")
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

    pub fn send_stdin_eof_blocking(&self, shell_id: &str) -> Result<bool> {
        let input = {
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
            input
        };
        drop(input);
        Ok(true)
    }

    pub fn read_stdout_chunk_blocking(
        &self,
        shell_id: &str,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        let output = self.output_for(shell_id)?;
        let Some(output) = output else {
            return Ok(None);
        };
        let mut output = output
            .lock()
            .map_err(|_| anyhow!("native output lock poisoned"))?;
        output.read_chunk(timeout)
    }

    pub fn read_line_blocking(&self, shell_id: &str, timeout: Duration) -> Result<Option<String>> {
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

    fn output_for(&self, shell_id: &str) -> Result<Option<Arc<Mutex<DirectOutput>>>> {
        let state = self.lock_state()?;
        let Some(entry) = state.entries.get(shell_id) else {
            return Ok(None);
        };
        Ok(entry.output.clone())
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
            },
        );
        Ok(())
    }

    fn next_shell_id(&self) -> Result<String> {
        let mut state = self.lock_state()?;
        state.next_id += 1;
        Ok(format!("frs_{}_{}", now_ms(), state.next_id))
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

impl FerrousNativeEnv {
    pub fn from_process_env() -> Self {
        Self::from_process_env_with_secret(read_env_or_else(
            "FRAMEWORK_SHELLS_SECRET",
            generate_secret,
        ))
    }

    pub fn from_process_env_with_secret(secret: String) -> Self {
        Self {
            secret,
            run_id: read_env_or_else("FRAMEWORK_SHELLS_RUN_ID", generate_run_id),
            fws_socketio_url: nonempty_env("FRAMEWORK_SHELLS_FWS_SOCKETIO_URL"),
            te_framework_url: nonempty_env("TE_FRAMEWORK_URL"),
            extra: HashMap::new(),
        }
    }

    pub fn child_env_overlay(&self) -> HashMap<String, String> {
        let mut out = self.extra.clone();
        out.insert("FRAMEWORK_SHELLS_SECRET".to_owned(), self.secret.clone());
        out.insert("FRAMEWORK_SHELLS_RUN_ID".to_owned(), self.run_id.clone());
        if let Some(url) = &self.fws_socketio_url {
            out.insert("FRAMEWORK_SHELLS_FWS_SOCKETIO_URL".to_owned(), url.clone());
        }
        if let Some(url) = &self.te_framework_url {
            out.insert("TE_FRAMEWORK_URL".to_owned(), url.clone());
        }
        out
    }
}

impl FerrousNativeStore {
    pub fn from_process_env() -> Result<Self> {
        let base_dir = fws_base_dir()?;
        let repo_fingerprint = fws_repo_fingerprint()?;
        let secret = match nonempty_env("FRAMEWORK_SHELLS_SECRET") {
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
            return Ok(None);
        }
        let mut out = Vec::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            match self.reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    self.log.write_all(&buffer[..n])?;
                    out.extend_from_slice(&buffer[..n]);
                    if n < buffer.len() {
                        break;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }
        self.log.flush()?;
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
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

fn record_path_for(log_dir: &std::path::Path, shell_id: &str) -> PathBuf {
    log_dir.join(format!("{shell_id}.record.json"))
}

pub fn load_persisted_record(path: impl AsRef<Path>) -> Result<FerrousNativeShellRecord> {
    let path = path.as_ref();
    let raw = read_to_string(path)
        .with_context(|| format!("failed to read native record {}", path.display()))?;
    let persisted = serde_json::from_str::<PersistedNativeShellRecord>(&raw)
        .with_context(|| format!("failed to parse native record {}", path.display()))?;
    let mut capabilities = persisted.capabilities;
    capabilities.stdin_write = false;
    capabilities.stdin_eof = false;
    capabilities.stdout_subscribe = false;
    capabilities.stderr_subscribe = false;
    capabilities.output_read = false;
    capabilities.terminate = false;
    capabilities.resize = false;
    Ok(FerrousNativeShellRecord {
        id: persisted.id,
        backend: persisted.backend,
        command: persisted.command,
        cwd: persisted.cwd.map(PathBuf::from),
        env: HashMap::new(),
        env_keys: persisted.env_keys,
        pid: persisted.pid,
        status: persisted.status,
        exit_code: persisted.exit_code,
        label: persisted.label,
        spec_id: persisted.spec_id,
        subgroups: persisted.subgroups,
        record_path: PathBuf::from(persisted.record_path),
        stdout_log: PathBuf::from(persisted.stdout_log),
        stderr_log: PathBuf::from(persisted.stderr_log),
        capabilities,
        adopted: true,
        created_at_ms: persisted.created_at_ms,
        updated_at_ms: persisted.updated_at_ms,
    })
}

fn list_persisted_records_in_dir(logs_dir: &Path) -> Result<Vec<FerrousNativeShellRecord>> {
    if !logs_dir.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in read_dir(logs_dir)
        .with_context(|| format!("failed to read native record dir {}", logs_dir.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "failed to read native record dir entry {}",
                logs_dir.display()
            )
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        if !path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.ends_with(".record.json"))
        {
            continue;
        }
        records.push(load_persisted_record(path)?);
    }
    records.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(records)
}

fn persist_record(record: &FerrousNativeShellRecord, path: &std::path::Path) -> Result<()> {
    let persisted = PersistedNativeShellRecord {
        id: record.id.clone(),
        spec_id: record.spec_id.clone(),
        backend: record.backend.clone(),
        command: record.command.clone(),
        cwd: record.cwd.as_ref().map(path_to_string),
        pid: record.pid,
        status: record.status.clone(),
        exit_code: record.exit_code,
        label: record.label.clone(),
        subgroups: record.subgroups.clone(),
        record_path: path_to_string(&record.record_path),
        stdout_log: path_to_string(&record.stdout_log),
        stderr_log: path_to_string(&record.stderr_log),
        io_metadata_log: None,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        run_id: record.env.get("FRAMEWORK_SHELLS_RUN_ID").cloned(),
        launcher_pid: std::process::id(),
        env_keys: record.env_keys.clone(),
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

fn path_to_string(path: &std::path::PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn sorted_env_keys(env: &HashMap<String, String>) -> Vec<String> {
    let mut keys = env.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys
}

fn set_nonblocking(fd: &impl AsFd) -> Result<()> {
    let flags = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
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
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
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

fn reactor_loop(state: Arc<Mutex<ManagerState>>, rx: Receiver<ReactorCommand>) {
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
            check_reactor_children(&state, &mut children);
            continue;
        }

        drain_reactor_commands(&rx, &mut streams, &mut children);
        poll_reactor_streams(&mut streams, Duration::from_millis(25));
        check_reactor_children(&state, &mut children);
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

fn poll_reactor_streams(streams: &mut Vec<ReactorLogStream>, timeout: Duration) {
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
        if !drain_reactor_stream(&mut streams[index]) {
            streams.swap_remove(index);
        }
    }
}

fn drain_reactor_stream(stream: &mut ReactorLogStream) -> bool {
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

fn check_reactor_children(state: &Arc<Mutex<ManagerState>>, children: &mut Vec<ReactorChild>) {
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
        if let Ok(mut state) = state.lock() {
            if let Some(entry) = state.entries.get_mut(&child_entry.shell_id) {
                entry.record.status = FerrousNativeShellStatus::Exited;
                entry.record.exit_code = Some(exit_code);
                entry.record.updated_at_ms = now_ms();
                let _ = persist_record(&entry.record, &entry.record_path);
            }
        }
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
