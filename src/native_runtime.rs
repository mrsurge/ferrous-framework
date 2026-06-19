use anyhow::{Context, Result, anyhow, bail};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    poll::{PollFd, PollFlags, PollTimeout, poll},
    pty::openpty,
    unistd::dup,
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all},
    io::{BufWriter, Read, Write},
    os::fd::{AsFd, AsRawFd, BorrowedFd},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FerrousNativeShellStatus {
    Running,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeShellCapabilities {
    pub stdin_write: bool,
    pub stdout_log: bool,
    pub stderr_log: bool,
    pub terminate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FerrousNativeShellRecord {
    pub id: String,
    pub backend: String,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub pid: u32,
    pub status: FerrousNativeShellStatus,
    pub exit_code: Option<i32>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub capabilities: FerrousNativeShellCapabilities,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

#[derive(Clone, Debug)]
pub struct FerrousNativeProcConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct FerrousNativePipeConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct FerrousNativePtyConfig {
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub label: String,
    pub spec_id: String,
    pub subgroups: Vec<String>,
    pub log_dir: PathBuf,
}

#[derive(Clone, Default)]
pub struct FerrousNativeManager {
    state: Arc<Mutex<ManagerState>>,
}

#[derive(Default)]
struct ManagerState {
    next_id: u64,
    entries: HashMap<String, NativeShellEntry>,
}

struct NativeShellEntry {
    record: FerrousNativeShellRecord,
    child: Arc<Mutex<Child>>,
    input: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    output: Option<Arc<Mutex<DirectOutput>>>,
}

struct DirectOutput {
    reader: Box<dyn ReadAsFd>,
    log: BufWriter<File>,
    line_buffer: Vec<u8>,
}

trait ReadAsFd: Read + AsRawFd + Send {}
impl<T: Read + AsRawFd + Send> ReadAsFd for T {}

impl FerrousNativeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn_proc_blocking(
        &self,
        config: FerrousNativeProcConfig,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native proc")?;
        create_dir_all(&config.log_dir).with_context(|| {
            format!(
                "failed to create native proc log directory {}",
                config.log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = config.log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = config.log_dir.join(format!("{id}.stderr.log"));
        let stdout_file = File::create(&stdout_log)
            .with_context(|| format!("failed to create stdout log {}", stdout_log.display()))?;
        let stderr_file = File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&config.env);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn native proc shell {id}"))?;
        let pid = child.id();
        if let Some(stdout) = child.stdout.take() {
            spawn_log_thread(stdout, stdout_file);
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_thread(stderr, stderr_file);
        }

        let now = now_ms();
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "proc".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env: config.env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: false,
                stdout_log: true,
                stderr_log: true,
                terminate: true,
            },
            created_at_ms: now,
            updated_at_ms: now,
        };
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(id.clone(), record.clone(), Arc::clone(&child), None, None)?;
        spawn_exit_watcher(Arc::clone(&self.state), id, child);
        Ok(record)
    }

    pub fn spawn_pipe_blocking(
        &self,
        config: FerrousNativePipeConfig,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native pipe")?;
        create_dir_all(&config.log_dir).with_context(|| {
            format!(
                "failed to create native pipe log directory {}",
                config.log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = config.log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = config.log_dir.join(format!("{id}.stderr.log"));
        let stdout_file = open_log(&stdout_log)?;
        let stderr_file = File::create(&stderr_log)
            .with_context(|| format!("failed to create stderr log {}", stderr_log.display()))?;

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&config.env);
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
            spawn_log_thread(stderr, stderr_file);
        }

        let now = now_ms();
        let record = FerrousNativeShellRecord {
            id: id.clone(),
            backend: "pipe".to_owned(),
            command: config.command,
            cwd: config.cwd,
            env: config.env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: input.is_some(),
                stdout_log: true,
                stderr_log: true,
                terminate: true,
            },
            created_at_ms: now,
            updated_at_ms: now,
        };
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(
            id.clone(),
            record.clone(),
            Arc::clone(&child),
            input,
            output,
        )?;
        spawn_exit_watcher(Arc::clone(&self.state), id, child);
        Ok(record)
    }

    pub fn spawn_pty_blocking(
        &self,
        config: FerrousNativePtyConfig,
    ) -> Result<FerrousNativeShellRecord> {
        validate_command(&config.command, "native pty")?;
        create_dir_all(&config.log_dir).with_context(|| {
            format!(
                "failed to create native pty log directory {}",
                config.log_dir.display()
            )
        })?;
        let id = self.next_shell_id()?;
        let stdout_log = config.log_dir.join(format!("{id}.stdout.log"));
        let stderr_log = config.log_dir.join(format!("{id}.stderr.log"));
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

        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command.envs(&config.env);
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
            env: config.env,
            pid,
            status: FerrousNativeShellStatus::Running,
            exit_code: None,
            label: config.label,
            spec_id: config.spec_id,
            subgroups: config.subgroups,
            stdout_log,
            stderr_log,
            capabilities: FerrousNativeShellCapabilities {
                stdin_write: true,
                stdout_log: true,
                stderr_log: false,
                terminate: true,
            },
            created_at_ms: now,
            updated_at_ms: now,
        };
        let child = Arc::new(Mutex::new(child));
        self.insert_entry(
            id.clone(),
            record.clone(),
            Arc::clone(&child),
            input,
            output,
        )?;
        spawn_exit_watcher(Arc::clone(&self.state), id, child);
        Ok(record)
    }

    pub fn list_shells(&self) -> Result<Vec<FerrousNativeShellRecord>> {
        let state = self.lock_state()?;
        let mut records = state
            .entries
            .values()
            .map(|entry| entry.record.clone())
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }

    pub fn get_shell(&self, shell_id: &str) -> Result<Option<FerrousNativeShellRecord>> {
        let state = self.lock_state()?;
        Ok(state
            .entries
            .get(shell_id)
            .map(|entry| entry.record.clone()))
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

    fn insert_entry(
        &self,
        id: String,
        record: FerrousNativeShellRecord,
        child: Arc<Mutex<Child>>,
        input: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
        output: Option<Arc<Mutex<DirectOutput>>>,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        state.entries.insert(
            id,
            NativeShellEntry {
                record,
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

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ManagerState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("native manager lock poisoned"))
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

fn spawn_log_thread(mut reader: impl Read + Send + 'static, file: File) {
    thread::spawn(move || {
        let mut file = BufWriter::with_capacity(256 * 1024, file);
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => {
                    let _ = file.flush();
                    return;
                }
                Ok(n) => {
                    if file.write_all(&buffer[..n]).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
}

fn spawn_exit_watcher(state: Arc<Mutex<ManagerState>>, shell_id: String, child: Arc<Mutex<Child>>) {
    thread::spawn(move || {
        loop {
            let exit_code = {
                let mut child = match child.lock() {
                    Ok(child) => child,
                    Err(_) => return,
                };
                match child.try_wait() {
                    Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
                    Ok(None) => None,
                    Err(_) => Some(-1),
                }
            };
            if let Some(exit_code) = exit_code {
                if let Ok(mut state) = state.lock() {
                    if let Some(entry) = state.entries.get_mut(&shell_id) {
                        entry.record.status = FerrousNativeShellStatus::Exited;
                        entry.record.exit_code = Some(exit_code);
                        entry.record.updated_at_ms = now_ms();
                    }
                }
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
    });
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
