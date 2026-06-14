#[cfg(feature = "pyo3-embed")]
mod pyo3_pipe {
    use anyhow::{Context, Result, anyhow};
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
    use std::{collections::HashMap, env, ffi::OsString, path::PathBuf, sync::Arc};

    pub const DEFAULT_PYTHON_MODULE: &str = "ferrous_framework.pipe";
    pub const DEFAULT_PYTHON_CLASS: &str = "FerrousFrameworkPipe";

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

    #[derive(Clone)]
    pub struct FerrousFrameworkPipe {
        inner: Arc<Py<PyAny>>,
    }

    impl FerrousFrameworkPipe {
        pub fn spawn(config: FerrousPipeConfig) -> Result<Self> {
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
                let cls = module.getattr(python_class.as_str())?;
                let command = PyList::new(py, &config.command)?;
                let env = PyDict::new(py);
                for (key, value) in config.env {
                    env.set_item(key, value)?;
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
                let object = cls.call1((
                    command,
                    cwd,
                    env,
                    config.label,
                    config.spec_id,
                    subgroups,
                    shellspec_path,
                    shellspec_entry,
                ))?;
                Ok(Self {
                    inner: Arc::new(object.into()),
                })
            })
            .map_err(|err| anyhow!("failed to start ferrous_framework pipe: {err}"))
        }

        pub fn write_line_blocking(&self, line: &str) -> Result<()> {
            Python::attach(|py| -> PyResult<()> {
                self.inner.call_method1(py, "write_line", (line,))?;
                Ok(())
            })
            .map_err(|err| anyhow!("ferrous_framework pipe write failed: {err}"))
        }

        pub fn read_line_blocking(&self) -> Result<Option<String>> {
            Python::attach(|py| -> PyResult<Option<String>> {
                self.inner
                    .call_method1(py, "read_line", (None::<f64>,))?
                    .extract(py)
            })
            .map_err(|err| anyhow!("ferrous_framework pipe read failed: {err}"))
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
            .map_err(|err| anyhow!("ferrous_framework pipe close failed: {err}"))
        }
    }
}

#[cfg(not(feature = "pyo3-embed"))]
mod pyo3_pipe {
    use anyhow::{Result, bail};
    use std::{collections::HashMap, path::PathBuf};

    pub const DEFAULT_PYTHON_MODULE: &str = "ferrous_framework.pipe";
    pub const DEFAULT_PYTHON_CLASS: &str = "FerrousFrameworkPipe";

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

pub use pyo3_pipe::{
    DEFAULT_PYTHON_CLASS, DEFAULT_PYTHON_MODULE, FerrousFrameworkPipe, FerrousPipeConfig,
};

pub const fn pyo3_embed_enabled() -> bool {
    cfg!(feature = "pyo3-embed")
}
