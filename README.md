# ferrous-framework

`ferrous-framework` is a Rust runtime crate for framework-shells-compatible process management.

Current direction: a Rust-compiled FWS-compatible manager/runtime suite with Rust-owned `proc`, `pipe`, and `pty` support. Python is not part of the crate runtime path.

## Native Runtime Shape

```text
Rust application
  -> ferrous-framework crate
  -> Rust-owned proc/pipe/pty runtime
  -> FWS-compatible records, logs, capabilities, and lifecycle metadata
```

## Public API

Current native API:

- `FerrousNativeManager`
- `FerrousNativeEnv`
- `FerrousNativeStore`
- `FerrousNativeProcConfig`
- `FerrousNativePipeConfig`
- `FerrousNativePtyConfig`
- `FerrousNativeShellRecord`
- `FerrousNativeShellStatus`
- `FerrousNativeShellCapabilities`
- `load_persisted_record`
- `shellspec::render_shellspec_entry`
- `FerrousNativeManager::spawn_shellspec_entry_blocking`

## Native Backend Coverage

```text
proc: launch, stdout/stderr logs, list/get, terminate, wait
pipe: launch, direct stdin writes, direct stdout reads, stdout/stderr logs, list/get, terminate, wait
pty: launch, direct PTY writes, direct PTY reads, PTY output log, list/get, terminate, wait
```

The `pipe` and `pty` hot paths are direct fd paths. They do not use a Python bridge, a stdout pump queue, or a drain worker. Reads are caller-driven and tee output to the log as bytes are read.

`FerrousNativeManager::new()` uses the same store layout as Python FWS: `FRAMEWORK_SHELLS_BASE_DIR` or `~/.cache/framework_shells`, `runtimes/<repo_fingerprint>/<runtime_id>/logs`, where `runtime_id` is `sha256(secret)[:16]`. If a spawn config leaves `log_dir` as `None`, logs and sidecar records are written into that canonical FWS logs directory. `Some(path)` remains an explicit override.

Each native launch writes a sidecar record at `FerrousNativeShellRecord.record_path`, next to the stdout/stderr logs. The sidecar records command/backend/status/log paths/capabilities/run metadata and env keys, but does not persist env values or secrets.

Fresh managers can load sidecar records from the canonical store logs directory. Loaded records are marked `adopted: true`, keep log/capability metadata for inspection, and deliberately clear live-only controls such as `stdin_write` and `terminate`.

## FWS Environment Contract

`FerrousNativeManager::new()` derives the FWS child environment from the current process:

- `FRAMEWORK_SHELLS_SECRET`
- `FRAMEWORK_SHELLS_RUN_ID`
- `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`
- `TE_FRAMEWORK_URL`

If `FRAMEWORK_SHELLS_SECRET` is absent, Ferrous follows the FWS CLI bootstrap shape: it loads `runtimes/<repo_fingerprint>/secret` when present, otherwise generates a `temporary_secret_<hex>` value and stores it there with owner-only permissions where supported. If `FRAMEWORK_SHELLS_RUN_ID` is absent, Ferrous generates a native run id. URL values are optional until a host/dashboard runtime is attached.

For explicit host control, construct the manager with `FerrousNativeManager::with_env(FerrousNativeEnv { ... })`. Every native `proc`, `pipe`, and `pty` spawn receives that overlay, then the shell config `env` is applied last so shell-specific overrides still work.

## Example

```rust
use ferrous_framework::{FerrousNativeEnv, FerrousNativeManager, FerrousNativePipeConfig};
use std::{collections::HashMap, path::PathBuf, time::Duration};

let manager = FerrousNativeManager::with_env(FerrousNativeEnv {
    secret: "dev-secret".into(),
    run_id: "dev-run".into(),
    fws_socketio_url: Some("http://127.0.0.1:9099/fws_ws".into()),
    te_framework_url: Some("http://127.0.0.1:9099".into()),
    extra: HashMap::new(),
});
let shell = manager.spawn_pipe_blocking(FerrousNativePipeConfig {
    command: vec!["sh".into(), "-c".into(), "while read line; do echo ack:$line; done".into()],
    cwd: None,
    env: HashMap::new(),
    label: "worker".into(),
    spec_id: "worker".into(),
    subgroups: vec!["demo".into()],
    log_dir: None,
})?;

manager.write_line_blocking(&shell.id, r#"{"jsonrpc":"2.0","id":1}"#)?;
let response = manager.read_line_blocking(&shell.id, Duration::from_secs(5))?;
```

## Shellspec Compatibility

Shellspec compatibility remains a core requirement. A compiled Rust framework should be able to change runtime parameters without rebuilding the binary.

The crate carries shellspec-rendering parity fixtures under `testdata/`. Run them with:

```sh
cargo test
```

The current fixture set covers `proc`, `pipe`, and `pty` render surfaces, ctx/env precedence, missing values, and stable free-port substitution.

Ferrous can also launch a rendered shellspec entry directly through `FerrousNativeManager::spawn_shellspec_entry_blocking(...)`. The API renders the selected entry, parses command/env/subgroups/backend, and dispatches to native `proc`, `pipe`, or `pty`. Shellspec `command` may be a string array or a shell-style command string.

## Test/Bench Signals

The native pipe tests include JSON-RPC-shaped request/response coverage with interleaved notifications and visible timing output when run with `-- --nocapture`:

```sh
cargo test pipe_ -- --nocapture
```
