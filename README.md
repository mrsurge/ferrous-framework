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
- `FerrousNativePtyMode`
- `FerrousNativeShellRecord`
- `FerrousNativeShellStatus`
- `FerrousNativeShellCapabilities`
- `FerrousNativeHost`
- `FerrousNativeHostConfig`
- `FerrousFrameworkPipe`
- `FerrousPipeConfig`
- `FerrousNativePipeState`
- `FerrousShellInputResult`
- `derive_native_api_token`
- `load_persisted_record`
- `pyo3_embed_enabled`
- `ferrous_native_enabled`
- `shellspec::render_shellspec_entry`
- `FerrousNativeManager::spawn_shellspec_entry_blocking`

## Native Backend Coverage

```text
proc: launch, stdout/stderr logs, list/get, terminate, wait
pipe: launch, direct stdin writes/EOF, direct stdout reads, stdout/stderr logs, list/get, terminate, wait
pty: launch, direct PTY writes/EOF, direct PTY reads, PTY resize, PTY output log, list/get, terminate, wait
```

The `pipe` and `pty` hot paths are direct fd paths. They do not use a Python bridge, a stdout pump queue, or a drain worker. Reads are caller-driven and tee output to the log as bytes are read.

PTY launch supports explicit terminal mode control through `FerrousNativePtyMode::{Interactive, Raw}`. `Raw` applies `cfmakeraw(...)` to the PTY slave before spawning the child; shellspec `pty_mode` is honored by native launch.

The base native PTY backend is a raw PTY byte-stream backend. The JSONL-out / JSON-RPC-in terminal-stream broker protocol is deferred and should be added later as an explicit higher-level PTY protocol mode/backend if a consumer needs it.

Passive log capture and child exit status are owned by one manager reactor thread instead of per-stream/per-shell helper threads. Pipe and PTY stdout remain caller-driven direct reads; the reactor handles proc stdout/stderr, pipe stderr, and child status persistence.

Output subscription is available through `subscribe_output(shell_id, stream, capacity)`. Subscriptions are bounded: if a subscriber does not drain its queue and a new chunk would exceed capacity, the runtime drops that subscriber instead of buffering unbounded output. Reactor-owned streams publish as they are logged; pipe/PTY stdout publish when the direct read path drains bytes.

The manager also exposes a Python-FWS-shaped async facade for downstream Rust callers that previously used the Python bridge shape:

- `spawn_shell(...)`
- `spawn_shell_pipe(...)`
- `spawn_shell_pty(...)`
- `get_pipe_state(...)`
- `write_to_pipe(...)`
- `write_to_shell(...)`
- `send_shell_eof(...)`
- `terminate_shell(...)`
- `subscribe_output_bytes(...)`

These methods are compatibility names over the same native runtime. They do not add JSON-RPC framing, app protocol routing, or Python networking behavior.

The async write/EOF compatibility methods stay on the direct native path. They do not call `tokio::task::spawn_blocking(...)` per packet; lifecycle-heavy operations such as spawn/terminate can still use blocking-task boundaries.

For ALS-style pipe consumers, `FerrousFrameworkPipe::spawn(FerrousPipeConfig { ... })` provides the legacy blocking adapter shape: `shell_id()`, `write_line_blocking(...)`, `read_line_blocking()`, and `close_blocking()`. It is intentionally pipe-only. If a shellspec renders to a non-pipe backend, the adapter errors instead of pretending line-oriented pipe semantics are available.

`pyo3_embed_enabled()` is retained as a legacy availability gate for older callers. In the native crate it reports the compatibility surface is available; it does not mean Python is in the runtime path. `ferrous_native_enabled()` is the literal native runtime capability flag.

`FerrousNativeManager::new()` uses the same store layout as Python FWS: `FRAMEWORK_SHELLS_BASE_DIR` or `~/.cache/framework_shells`, `runtimes/<repo_fingerprint>/<runtime_id>/logs`, where `runtime_id` is `sha256(secret)[:16]`. If a spawn config leaves `log_dir` as `None`, logs and sidecar records are written into that canonical FWS logs directory. `Some(path)` remains an explicit override.

Each native launch writes a sidecar record at `FerrousNativeShellRecord.record_path`, next to the stdout/stderr logs. The sidecar records command/backend/status/log paths/capabilities/run metadata and env keys, but does not persist env values or secrets. The JSON includes FWS-compatible fields such as `created_at`, `updated_at`, `autostart`, `ui`, `debug`, `io_metadata_log`, backend flags, `runtime_id`, and derived app metadata. `io_metadata_log` is currently a stable sidecar path, not proof that Ferrous is writing IO metadata records yet.

Fresh managers can load sidecar records from the canonical store logs directory. Loaded records are marked `adopted: true`, keep log/capability metadata for inspection, and deliberately clear live-only controls such as `stdin_write` and `terminate`.

Capability records distinguish logs, live output reads, output subscriptions, stdin write, stdin EOF, terminate, and resize explicitly. PTY shells expose `resize_pty_blocking(...)` through the native manager. Adopted/stale records clear live-only controls even if the persisted record was created by a live owner.

## Native Host / Control Plane

`FerrousNativeHost` is the first Rust-owned FWS host MVP. It runs an Axum/Tokio HTTP server around `FerrousNativeManager` and does not depend on Python.

Current host surfaces:

- `GET /health`
- `GET /fws`
- `GET /api/framework_shells/runtime`
- `GET /api/framework_shells`
- `POST /api/framework_shells`
- `POST /api/framework_shells/shellspec/apply`
- `GET /api/framework_shells/{shell_id}`
- `POST /api/framework_shells/{shell_id}/terminate`
- `POST /api/framework_shells/{shell_id}/action`
- `POST /api/framework_shells/{shell_id}/input`
- `POST /api/framework_shells/app/{app_id}/shutdown`
- `GET /api/framework_shells/logs/{shell_id}/tail`

Mutating routes require the same API token shape as Python FWS: `HMAC(secret, "api")`, passed as `X-Framework-Key` or `Authorization: Bearer ...`. `derive_native_api_token(...)` exposes that token derivation for Rust callers and tests.

The MVP host reports `socketio: false`; it is an HTTP/dashboard/control-plane root, not a Socket.IO-compatible peer lane yet. Group shutdown currently terminates Ferrous-owned live shell roots for the derived app/group id and returns the shutdown DTO shape.

## FWS Environment Contract

`FerrousNativeManager::new()` derives the FWS child environment from the current process:

- `FRAMEWORK_SHELLS_SECRET`
- `FRAMEWORK_SHELLS_RUN_ID`
- `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`
- `TE_FRAMEWORK_URL`

If `FRAMEWORK_SHELLS_SECRET` is absent, Ferrous follows the FWS CLI bootstrap shape: it loads `runtimes/<repo_fingerprint>/secret` when present, otherwise generates a `temporary_secret_<hex>` value and stores it there with owner-only permissions where supported. If `FRAMEWORK_SHELLS_RUN_ID` is absent, Ferrous generates a native run id. URL values are optional until a host/dashboard runtime is attached.

When `FerrousNativeHost::spawn(...)` creates the manager, it sets `TE_FRAMEWORK_URL` to the bound host URL when the value is otherwise absent. The MVP host intentionally does not set `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL` until a real Socket.IO-compatible lane exists.

For explicit host control, construct the manager with `FerrousNativeManager::with_env(FerrousNativeEnv { ... })`. Every native `proc`, `pipe`, and `pty` spawn receives that overlay, then the shell config `env` is applied last so shell-specific overrides still work.

When a caller already has an explicit FWS environment map, use `FerrousNativeManager::try_with_env_map(...)` or `with_env_map(...)`. The env map can supply `FRAMEWORK_SHELLS_BASE_DIR`, `FRAMEWORK_SHELLS_REPO_FINGERPRINT`, `FRAMEWORK_SHELLS_SECRET`, `FRAMEWORK_SHELLS_RUN_ID`, `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`, and `TE_FRAMEWORK_URL`; missing values fall back to the process environment and normal stored-secret bootstrap.

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

Ferrous can also launch a rendered shellspec entry directly through `FerrousNativeManager::spawn_shellspec_entry_blocking(...)`. The API renders the selected entry, parses command/env/subgroups/backend, and dispatches to native `proc`, `pipe`, or `pty`. Shellspec `command` may be a string array or a shell-style command string. Direct launch waits for supported readiness probes (`tcp_port`, `stdout_regex`) and rejects unsupported probe types explicitly.

For multi-entry shellspec documents, `FerrousNativeManager::apply_shellspec_document_blocking(...)` starts missing `autostart` specs, skips live running specs with the same `spec_id`, and can prune live specs that are no longer present in the desired document.

## Test/Bench Signals

The native pipe tests include JSON-RPC-shaped request/response coverage with interleaved notifications and visible timing output when run with `-- --nocapture`:

```sh
cargo test pipe_ -- --nocapture
```

The PTY terminal path has a focused request/response timing smoke:

```sh
cargo test pty_terminal -- --nocapture
```

The async facade performance probes are opt-in ignored tests, so they do not run during default correctness checks:

```sh
cargo test --release pipe_async_facade_reports_rtt_overhead_against_blocking_direct -- --ignored --nocapture
cargo test --release pipe_async_facade_reports_concurrent_inflight_metrics -- --ignored --nocapture
```
