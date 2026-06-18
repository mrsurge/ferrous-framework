use anyhow::{Context, Result, anyhow, bail};
use std::{
    collections::HashMap,
    fs::{File, create_dir_all},
    io::{Read, Write},
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

#[derive(Clone, Debug, Default)]
pub struct FerrousNativeManager {
    state: Arc<Mutex<ManagerState>>,
}

#[derive(Debug, Default)]
struct ManagerState {
    next_id: u64,
    entries: HashMap<String, NativeShellEntry>,
}

#[derive(Debug)]
struct NativeShellEntry {
    record: FerrousNativeShellRecord,
    child: Arc<Mutex<Child>>,
}

impl FerrousNativeManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn_proc_blocking(
        &self,
        config: FerrousNativeProcConfig,
    ) -> Result<FerrousNativeShellRecord> {
        if config.command.is_empty() {
            bail!("native proc command cannot be empty");
        }
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
        {
            let mut state = self.lock_state()?;
            state.entries.insert(
                id.clone(),
                NativeShellEntry {
                    record: record.clone(),
                    child: Arc::clone(&child),
                },
            );
        }
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
            .map_err(|_| anyhow!("native proc child lock poisoned"))?;
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        Ok(true)
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

    fn next_shell_id(&self) -> Result<String> {
        let mut state = self.lock_state()?;
        state.next_id += 1;
        Ok(format!("frs_{}_{}", now_ms(), state.next_id))
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ManagerState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("native proc manager lock poisoned"))
    }
}

fn spawn_log_thread(mut reader: impl Read + Send + 'static, mut file: File) {
    thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
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
                    let _ = file.flush();
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
