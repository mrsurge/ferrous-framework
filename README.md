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

That module is provided by `framework-shells >= 0.0.55`.

## Public API

Generic API:

- `FerrousBackend`
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

The current bridge carries shellspec path and entry information through to Python FWS. Future Rust-native work should preserve shellspec behavior and eventually add Rust-side parsing/rendering parity.

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
- The crate expects the Python environment to have `framework-shells >= 0.0.55` when `pyo3-embed` is used.
- ALS-RS is a pipe compatibility consumer, not the full target architecture.
- TE2-style framework runtimes are the broader compatibility canary for future Rust-owned FWS behavior.

## Roadmap

- Add capability DTOs.
- Add Rust-owned pipe runtime.
- Add Rust-owned PTY runtime.
- Add Rust-owned proc lifecycle/log runtime.
- Add Rust shellspec parser/renderer parity.
- Build toward a Rust FWS-compatible manager suite.
