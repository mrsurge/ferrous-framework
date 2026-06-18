pub mod bridge;
pub mod native_proc;
pub mod shellspec;
pub mod shutdown;

#[cfg(feature = "pyo3-embed")]
mod pyo3_bridge {
    use crate::bridge::validate_bridge_info;
    use crate::shutdown::{FerrousShutdownResult, parse_shutdown_result};
    use anyhow::{Context, Result, anyhow};
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PyString};
    use serde_json::Value;
    use std::{collections::HashMap, env, ffi::OsString, path::PathBuf, sync::Arc};

    pub const DEFAULT_PYTHON_MODULE: &str = "framework_shells.ferrous_framework";
    pub const DEFAULT_PYTHON_CLASS: &str = "FerrousFrameworkPipe";
    pub const DEFAULT_PYTHON_HOST_CLASS: &str = "FerrousFrameworkHost";

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum FerrousBackend {
        #[default]
        Pipe,
        Pty,
        Proc,
    }

    impl FerrousBackend {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Pipe => "pipe",
                Self::Pty => "pty",
                Self::Proc => "proc",
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct FerrousPipeConfig {
        pub command: Vec<String>,
        pub cwd: Option<PathBuf>,
        pub env: HashMap<String, String>,
        pub label: String,
        pub spec_id: String,
        pub subgroups: Vec<String>,
        pub shellspec_path: Option<PathBuf>,
        pub shellspec_entry: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct FerrousShellConfig {
        pub backend: FerrousBackend,
        pub command: Vec<String>,
        pub cwd: Option<PathBuf>,
        pub env: HashMap<String, String>,
        pub label: String,
        pub spec_id: String,
        pub subgroups: Vec<String>,
        pub ctx: HashMap<String, String>,
        pub shellspec_path: Option<PathBuf>,
        pub shellspec_entry: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    impl From<FerrousPipeConfig> for FerrousShellConfig {
        fn from(config: FerrousPipeConfig) -> Self {
            Self {
                backend: FerrousBackend::Pipe,
                command: config.command,
                cwd: config.cwd,
                env: config.env,
                label: config.label,
                spec_id: config.spec_id,
                subgroups: config.subgroups,
                ctx: HashMap::new(),
                shellspec_path: config.shellspec_path,
                shellspec_entry: config.shellspec_entry,
                python_module: config.python_module,
                python_class: config.python_class,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct FerrousHostConfig {
        pub host: String,
        pub port: u16,
        pub env: HashMap<String, String>,
        pub run_id: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    impl Default for FerrousHostConfig {
        fn default() -> Self {
            Self {
                host: "127.0.0.1".to_owned(),
                port: 0,
                env: HashMap::new(),
                run_id: None,
                python_module: None,
                python_class: None,
            }
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkHost {
        inner: Arc<Py<PyAny>>,
    }

    impl FerrousFrameworkHost {
        pub fn spawn(config: FerrousHostConfig) -> Result<Self> {
            let pythonpath = config.env.get("PYTHONPATH").map(OsString::from);
            let python_module = config
                .python_module
                .as_deref()
                .unwrap_or(DEFAULT_PYTHON_MODULE)
                .to_owned();
            let python_class = config
                .python_class
                .as_deref()
                .unwrap_or(DEFAULT_PYTHON_HOST_CLASS)
                .to_owned();
            Python::initialize();
            Python::attach(|py| -> PyResult<Self> {
                if let Some(pythonpath) = pythonpath {
                    let sys = py.import("sys")?;
                    let sys_path = sys.getattr("path")?;
                    let paths: Vec<_> = env::split_paths(&pythonpath).collect();
                    for path in paths.into_iter().rev() {
                        sys_path
                            .call_method1("insert", (0, path.to_string_lossy().into_owned()))?;
                    }
                }
                let module = py.import(python_module.as_str())?;
                validate_python_bridge(py, &module)?;
                let cls = module.getattr(python_class.as_str())?;
                let env = PyDict::new(py);
                for (key, value) in config.env {
                    env.set_item(key, value)?;
                }
                let object =
                    cls.call1((config.host, config.port, env, config.run_id.as_deref()))?;
                Ok(Self {
                    inner: Arc::new(object.into()),
                })
            })
            .map_err(|err| anyhow!("failed to start ferrous_framework host: {err}"))
        }

        pub fn url(&self) -> Result<String> {
            Python::attach(|py| -> PyResult<String> {
                self.inner.call_method0(py, "url")?.extract(py)
            })
            .context("failed to read ferrous_framework host url")
        }

        pub fn port(&self) -> Result<u16> {
            let port = Python::attach(|py| -> PyResult<u16> {
                self.inner.call_method0(py, "port")?.extract(py)
            })
            .context("failed to read ferrous_framework host port")?;
            Ok(port)
        }

        pub fn child_env(&self) -> Result<HashMap<String, String>> {
            Python::attach(|py| -> PyResult<HashMap<String, String>> {
                self.inner.call_method0(py, "child_env")?.extract(py)
            })
            .context("failed to read ferrous_framework host child env")
        }

        pub fn close_blocking(&self) -> Result<()> {
            Python::attach(|py| -> PyResult<()> {
                self.inner.call_method0(py, "close")?;
                Ok(())
            })
            .map_err(|err| anyhow!("ferrous_framework host close failed: {err}"))
        }

        pub fn shutdown_group_blocking(&self, app_id: &str) -> Result<FerrousShutdownResult> {
            Python::attach(|py| -> PyResult<FerrousShutdownResult> {
                let result = self.inner.call_method1(py, "shutdown_group", (app_id,))?;
                let value = py_any_to_json_value(py, result.bind(py))?;
                parse_shutdown_result(&value).map_err(|err| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "ferrous_framework shutdown_group returned invalid result: {err}"
                    ))
                })
            })
            .map_err(|err| anyhow!("ferrous_framework host shutdown_group failed: {err}"))
        }

        pub fn shutdown_tree_blocking(&self, root_pids: Vec<i64>) -> Result<FerrousShutdownResult> {
            Python::attach(|py| -> PyResult<FerrousShutdownResult> {
                let result = self.inner.call_method1(py, "shutdown_tree", (root_pids,))?;
                let value = py_any_to_json_value(py, result.bind(py))?;
                parse_shutdown_result(&value).map_err(|err| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "ferrous_framework shutdown_tree returned invalid result: {err}"
                    ))
                })
            })
            .map_err(|err| anyhow!("ferrous_framework host shutdown_tree failed: {err}"))
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkShell {
        inner: Arc<Py<PyAny>>,
    }

    impl FerrousFrameworkShell {
        pub fn spawn(config: FerrousShellConfig) -> Result<Self> {
            let pythonpath = config.env.get("PYTHONPATH").map(OsString::from);
            let python_module = config
                .python_module
                .as_deref()
                .unwrap_or(DEFAULT_PYTHON_MODULE)
                .to_owned();
            let python_class = config
                .python_class
                .as_deref()
                .unwrap_or(DEFAULT_PYTHON_CLASS)
                .to_owned();
            Python::initialize();
            Python::attach(|py| -> PyResult<Self> {
                if let Some(pythonpath) = pythonpath {
                    let sys = py.import("sys")?;
                    let sys_path = sys.getattr("path")?;
                    let paths: Vec<_> = env::split_paths(&pythonpath).collect();
                    for path in paths.into_iter().rev() {
                        sys_path
                            .call_method1("insert", (0, path.to_string_lossy().into_owned()))?;
                    }
                }
                let module = py.import(python_module.as_str())?;
                validate_python_bridge(py, &module)?;
                let cls = module.getattr(python_class.as_str())?;
                let command = PyList::new(py, &config.command)?;
                let env = PyDict::new(py);
                for (key, value) in config.env {
                    env.set_item(key, value)?;
                }
                let ctx = PyDict::new(py);
                for (key, value) in config.ctx {
                    ctx.set_item(key, value)?;
                }
                let subgroups = PyList::new(py, &config.subgroups)?;
                let cwd = config
                    .cwd
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned());
                let shellspec_path = config
                    .shellspec_path
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned());
                let shellspec_entry = config.shellspec_entry.as_deref();
                let backend = config.backend.as_str();
                let object = cls.call1((
                    command,
                    cwd,
                    env,
                    config.label,
                    config.spec_id,
                    subgroups,
                    shellspec_path,
                    shellspec_entry,
                    backend,
                    ctx,
                ))?;
                Ok(Self {
                    inner: Arc::new(object.into()),
                })
            })
            .map_err(|err| anyhow!("failed to start ferrous_framework shell: {err}"))
        }

        pub fn write_line_blocking(&self, line: &str) -> Result<()> {
            Python::attach(|py| -> PyResult<()> {
                self.inner.call_method1(py, "write_line", (line,))?;
                Ok(())
            })
            .map_err(|err| anyhow!("ferrous_framework shell write failed: {err}"))
        }

        pub fn read_line_blocking(&self) -> Result<Option<String>> {
            Python::attach(|py| -> PyResult<Option<String>> {
                self.inner
                    .call_method1(py, "read_line", (None::<f64>,))?
                    .extract(py)
            })
            .map_err(|err| anyhow!("ferrous_framework shell read failed: {err}"))
        }

        pub fn shell_id(&self) -> Result<String> {
            Python::attach(|py| -> PyResult<String> {
                self.inner.call_method0(py, "shell_id")?.extract(py)
            })
            .context("failed to read ferrous_framework shell id")
        }

        pub fn close_blocking(&self) -> Result<()> {
            Python::attach(|py| -> PyResult<()> {
                self.inner.call_method0(py, "close")?;
                Ok(())
            })
            .map_err(|err| anyhow!("ferrous_framework shell close failed: {err}"))
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkPipe {
        inner: FerrousFrameworkShell,
    }

    impl FerrousFrameworkPipe {
        pub fn spawn(config: FerrousPipeConfig) -> Result<Self> {
            Ok(Self {
                inner: FerrousFrameworkShell::spawn(config.into())?,
            })
        }

        pub fn write_line_blocking(&self, line: &str) -> Result<()> {
            self.inner.write_line_blocking(line)
        }

        pub fn read_line_blocking(&self) -> Result<Option<String>> {
            self.inner.read_line_blocking()
        }

        pub fn shell_id(&self) -> Result<String> {
            self.inner.shell_id()
        }

        pub fn close_blocking(&self) -> Result<()> {
            self.inner.close_blocking()
        }
    }

    fn validate_python_bridge(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
        let info = module
            .getattr("ferrous_bridge_info")
            .and_then(|func| func.call0())
            .map_err(|err| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "installed ferrous Python bridge does not expose ferrous_bridge_info(): {err}"
                ))
            })?;
        let json = py.import("json")?;
        let value = py_any_to_json_value_with_json(
            &json,
            &info,
            "installed ferrous Python bridge returned invalid metadata",
        )?;
        validate_bridge_info(&value).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "installed ferrous Python bridge is incompatible: {err}"
            ))
        })?;
        Ok(())
    }

    fn py_any_to_json_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Value> {
        let json = py.import("json")?;
        py_any_to_json_value_with_json(&json, value, "failed to convert Python object to JSON")
    }

    fn py_any_to_json_value_with_json(
        json: &Bound<'_, PyModule>,
        value: &Bound<'_, PyAny>,
        context: &str,
    ) -> PyResult<Value> {
        let raw = json.call_method1("dumps", (value,))?;
        let raw = raw.cast::<PyString>()?.to_str()?;
        serde_json::from_str(raw).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{context}: {err}"))
        })
    }
}

#[cfg(not(feature = "pyo3-embed"))]
mod pyo3_bridge {
    use crate::shutdown::FerrousShutdownResult;
    use anyhow::{Result, bail};
    use std::{collections::HashMap, path::PathBuf};

    pub const DEFAULT_PYTHON_MODULE: &str = "framework_shells.ferrous_framework";
    pub const DEFAULT_PYTHON_CLASS: &str = "FerrousFrameworkPipe";
    pub const DEFAULT_PYTHON_HOST_CLASS: &str = "FerrousFrameworkHost";

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum FerrousBackend {
        #[default]
        Pipe,
        Pty,
        Proc,
    }

    impl FerrousBackend {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Pipe => "pipe",
                Self::Pty => "pty",
                Self::Proc => "proc",
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct FerrousPipeConfig {
        pub command: Vec<String>,
        pub cwd: Option<PathBuf>,
        pub env: HashMap<String, String>,
        pub label: String,
        pub spec_id: String,
        pub subgroups: Vec<String>,
        pub shellspec_path: Option<PathBuf>,
        pub shellspec_entry: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct FerrousShellConfig {
        pub backend: FerrousBackend,
        pub command: Vec<String>,
        pub cwd: Option<PathBuf>,
        pub env: HashMap<String, String>,
        pub label: String,
        pub spec_id: String,
        pub subgroups: Vec<String>,
        pub ctx: HashMap<String, String>,
        pub shellspec_path: Option<PathBuf>,
        pub shellspec_entry: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    impl From<FerrousPipeConfig> for FerrousShellConfig {
        fn from(config: FerrousPipeConfig) -> Self {
            Self {
                backend: FerrousBackend::Pipe,
                command: config.command,
                cwd: config.cwd,
                env: config.env,
                label: config.label,
                spec_id: config.spec_id,
                subgroups: config.subgroups,
                ctx: HashMap::new(),
                shellspec_path: config.shellspec_path,
                shellspec_entry: config.shellspec_entry,
                python_module: config.python_module,
                python_class: config.python_class,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct FerrousHostConfig {
        pub host: String,
        pub port: u16,
        pub env: HashMap<String, String>,
        pub run_id: Option<String>,
        pub python_module: Option<String>,
        pub python_class: Option<String>,
    }

    impl Default for FerrousHostConfig {
        fn default() -> Self {
            Self {
                host: "127.0.0.1".to_owned(),
                port: 0,
                env: HashMap::new(),
                run_id: None,
                python_module: None,
                python_class: None,
            }
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkHost;

    impl FerrousFrameworkHost {
        pub fn spawn(_config: FerrousHostConfig) -> Result<Self> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn url(&self) -> Result<String> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn port(&self) -> Result<u16> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn child_env(&self) -> Result<HashMap<String, String>> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn close_blocking(&self) -> Result<()> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn shutdown_group_blocking(&self, _app_id: &str) -> Result<FerrousShutdownResult> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn shutdown_tree_blocking(
            &self,
            _root_pids: Vec<i64>,
        ) -> Result<FerrousShutdownResult> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkShell;

    impl FerrousFrameworkShell {
        pub fn spawn(_config: FerrousShellConfig) -> Result<Self> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn write_line_blocking(&self, _line: &str) -> Result<()> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn read_line_blocking(&self) -> Result<Option<String>> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn shell_id(&self) -> Result<String> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn close_blocking(&self) -> Result<()> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }
    }

    #[derive(Clone)]
    pub struct FerrousFrameworkPipe;

    impl FerrousFrameworkPipe {
        pub fn spawn(_config: FerrousPipeConfig) -> Result<Self> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn write_line_blocking(&self, _line: &str) -> Result<()> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn read_line_blocking(&self) -> Result<Option<String>> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn shell_id(&self) -> Result<String> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }

        pub fn close_blocking(&self) -> Result<()> {
            bail!("ferrous_framework was built without the pyo3-embed feature")
        }
    }
}

pub use native_proc::{
    FerrousNativeManager, FerrousNativeProcConfig, FerrousNativeShellCapabilities,
    FerrousNativeShellRecord, FerrousNativeShellStatus,
};
pub use pyo3_bridge::{
    DEFAULT_PYTHON_CLASS, DEFAULT_PYTHON_HOST_CLASS, DEFAULT_PYTHON_MODULE, FerrousBackend,
    FerrousFrameworkHost, FerrousFrameworkPipe, FerrousFrameworkShell, FerrousHostConfig,
    FerrousPipeConfig, FerrousShellConfig,
};
pub use shutdown::{FerrousShutdownResult, FerrousShutdownStats};

pub const fn pyo3_embed_enabled() -> bool {
    cfg!(feature = "pyo3-embed")
}
