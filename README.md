# ferrous-framework

`ferrous-framework` is a Rust adapter crate for projects that want framework-shells-style process management from Rust.

Current baseline: it joins an existing Python `framework-shells` environment through a PyO3 bridge. This preserves FWS shared-secret handling, shell metadata, logs, dashboard visibility, and shellspec rendering while the Rust-native manager/runtime grows in the crate.

Target direction: a Rust-compiled FWS-compatible manager/runtime suite with Rust-owned `pipe`, `pty`, and `proc` support.

## Current Runtime Shape

```text
Rust application
  -> ferrous-framework crate
  -> PyO3 bridge
  -> installed framework-shells Python package
  -> FWS manager/dashboard/metadata/logs
```

The bridge imports this Python module by default:

```text
framework_shells.ferrous_framework
```

That module is provided by `framework-shells >= 0.0.57`.

## Native Proc Baseline

The first Rust-owned runtime slice is `FerrousNativeManager` with `proc` support:

```rust
use ferrous_framework::{
    FerrousNativeManager, FerrousNativeProcConfig,
};

let manager = FerrousNativeManager::new();
let record = manager.spawn_proc_blocking(FerrousNativeProcConfig {
    command: vec!["sh".into(), "-c".into(), "echo hello".into()],
    cwd: None,
    env: Default::default(),
    label: "worker".into(),
    spec_id: "worker".into(),
    subgroups: vec!["demo".into()],
    log_dir: "target/fws-logs".into(),
})?;
let exited = manager.wait_shell_blocking(
    &record.id,
    std::time::Duration::from_secs(5),
)?;
```

This path is fully Rust-owned: it launches the process, records shell metadata, captures stdout/stderr to log files, lists/gets records, terminates running children, and observes exit status. It does not use Python or the PyO3 bridge.

Current native backend coverage:

```text
proc: launch, logs, list/get, terminate, wait
pipe: bridge-backed only
pty: bridge-backed only
```

## Public API

Generic API:

- `FerrousBackend`
- `FerrousHostConfig`
- `FerrousFrameworkHost`
- `FerrousShellConfig`
- `FerrousFrameworkShell`

Compatibility pipe API:

- `FerrousPipeConfig`
- `FerrousFrameworkPipe`

The pipe names are kept for current consumers such as ALS-RS. New generic consumers should prefer the shell names.

## Backends

Ferrous models three FWS backend targets:

```text
pipe
pty
proc
```

Current implementation is still bridge-backed. Backend behavior is provided by the installed Python `framework-shells` manager.

## Shellspec Compatibility

Shellspec compatibility is a core requirement. A compiled Rust framework should be able to change runtime parameters without rebuilding the binary.

The current bridge carries shellspec path, entry, and render context information through to Python FWS. Future Rust-native work should preserve shellspec behavior and eventually add Rust-side parsing/rendering parity.

## Host Runtime

`FerrousFrameworkHost` starts the current bridge-backed FWS dashboard and Socket.IO root for Rust programs that are the FWS initializer. It binds `host:port`, supports `port: 0` for free-port parity, and returns child environment values that should be inherited by managed workers.

```rust
use ferrous_framework::{FerrousFrameworkHost, FerrousHostConfig};

let host = FerrousFrameworkHost::spawn(FerrousHostConfig::default())?;
let fws_url = host.url()?;
let child_env = host.child_env()?;
```

The current host implementation enters Python once and mounts the existing FWS FastAPI/Socket.IO runtime. Future Rust-native work should keep this public shape while replacing the internals with a Rust-owned host.

When the PyO3 bridge path is used, Ferrous validates the installed Python bridge metadata before constructing a host or shell. Older or incompatible `framework_shells.ferrous_framework` installs fail fast instead of silently taking the wrong bridge path.

The host also exposes shutdown management calls:

```rust
let group_result = host.shutdown_group_blocking("my-app")?;
let tree_result = host.shutdown_tree_blocking(vec![1234])?;
```

Both calls delegate to the installed framework-shells shutdown implementation and return `FerrousShutdownResult`, including timing, root PIDs, stats, and collected shutdown events. They do not replace the FWS shutdown algorithm.

## Feature Flags

By default, the crate builds without embedding Python and returns explicit errors from runtime calls.

Enable the PyO3 bridge with:

```toml
ferrous_framework = { version = "0.1", features = ["pyo3-embed"] }
```

## Example

```rust
use ferrous_framework::{
    FerrousBackend, FerrousFrameworkShell, FerrousShellConfig,
};
use std::{collections::HashMap, path::PathBuf};

let shell = FerrousFrameworkShell::spawn(FerrousShellConfig {
    backend: FerrousBackend::Pipe,
    command: vec!["python".into(), "-m".into(), "my_jsonrpc_worker".into()],
    cwd: Some(PathBuf::from("/path/to/project")),
    env: HashMap::new(),
    label: "my-worker".into(),
    spec_id: "my-worker".into(),
    subgroups: vec!["my-app".into(), "jsonrpc".into()],
    ctx: HashMap::new(),
    shellspec_path: None,
    shellspec_entry: None,
    python_module: None,
    python_class: None,
})?;

shell.write_line_blocking(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)?;
let response = shell.read_line_blocking()?;
```

## Compatibility Notes

- `FerrousFrameworkPipe` remains available as a compatibility wrapper around `FerrousFrameworkShell` with `FerrousBackend::Pipe`.
- The crate expects the Python environment to have `framework-shells >= 0.0.57` when `pyo3-embed` is used.
- ALS-RS is a pipe compatibility consumer, not the full target architecture.
- TE2-style framework runtimes are the broader compatibility canary for future Rust-owned FWS behavior.

## Roadmap

- Add capability DTOs.
- Add Rust-owned pipe runtime.
- Add Rust-owned PTY runtime.
- Add Rust-owned proc lifecycle/log runtime.
- Add Rust shellspec parser/renderer parity.
- Build toward a Rust FWS-compatible manager suite.

## Parity Tests

The crate carries shellspec-rendering parity fixtures under `testdata/`. Run them with:

```sh
cargo test
```

The current fixture set covers `proc`, `pipe`, and `pty` render surfaces, ctx/env precedence, missing values, and stable free-port substitution.
