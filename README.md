# ferrous-framework

`ferrous-framework` is the Rust implementation of the FWS process-runtime contract.

It gives Rust applications the same process-supervision model as Python [`framework-shells`](https://github.com/mrsurge/framework-shells): shellspec rendering, runtime-scoped metadata, logs, capabilities, dashboard/control-plane compatibility, peer-manager interop, and shutdown behavior.

Python is not part of the crate runtime path. The native manager owns `proc`, `pipe`, and `pty` execution directly in Rust.

Use `ferrous-framework` when a Rust host needs FWS-compatible process supervision, when Python should not sit on the hot path for process I/O, or when a compiled framework wants to keep runtime behavior configurable through shellspecs.

Use Python `framework-shells` when the host runtime is Python, when FastAPI/ASGI mounting is the primary integration point, or when the Python implementation is the better reference surface for a consumer.

## Native Runtime Shape

```text
Rust application
  -> ferrous-framework crate
  -> Rust-owned proc/pipe/pty runtime
  -> FWS-compatible records, logs, capabilities, and lifecycle metadata
```

The boundary matches FWS:

- Ferrous owns launch, shutdown, shellspec rendering, runtime metadata, logs, capabilities, dashboard/control surfaces, and peer-manager coordination.
- The application owns its protocol, request routing, DTOs, and business logic.

The `pipe` backend is a supervised stdin/stdout/stderr byte stream. Ferrous does not parse JSON-RPC, line protocols, editor control messages, or application DTOs unless a caller adds that layer above the runtime.

## What This Is Not

- It is not a Python bridge.
- It is not a protocol framework.
- It is not a terminal emulator.
- It is not a separate FWS dialect.

The target is interoperability: Python FWS and Ferrous managers should be able to share metadata, dashboard/control-plane semantics, shellspec conventions, and peer lanes while letting each host keep its own runtime implementation.

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
- `FerrousNativePeer`
- `FerrousNativePeerConfig`
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
- `FerrousNativeManager::spawn_shellspec_entry_with_overrides_blocking`
- `FerrousNativeManager::shutdown_tree_blocking`
- `FerrousNativeManager::shutdown_all_blocking`

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

`FerrousNativeManager::new()` uses the same store layout as Python FWS: `FRAMEWORK_SHELLS_BASE_DIR` or `~/.cache/framework_shells`, `runtimes/<repo_fingerprint>/<runtime_id>/meta`, `logs`, and `sockets`, where `runtime_id` is `sha256(secret)[:16]`. If a spawn config leaves `log_dir` as `None`, logs are written into the canonical FWS logs directory. `Some(path)` remains an explicit log override; metadata still goes to the canonical FWS metadata directory.

Each native launch writes a Python-FWS-shaped metadata record at `meta/<shell_id>/meta.json`, exposed as `FerrousNativeShellRecord.record_path`. The record includes command/backend/status/log paths/capabilities/run metadata, labels, subgroups, UI/debug metadata, explicit launch `env_overrides`, env keys, backend flags, `runtime_id`, and derived app metadata. Manager-owned FWS secrets inherited through the native environment overlay are not written into `env_overrides` unless a caller explicitly passes them as a shell override. `io_metadata_log` is currently a stable sidecar path, not proof that Ferrous is writing IO metadata records yet.

Fresh managers can load metadata records from the canonical store metadata directory. Loaded records are marked `adopted: true`, keep log/capability metadata for inspection, and deliberately clear live-only controls such as `stdin_write` and `terminate`.

Capability records distinguish logs, live output reads, output subscriptions, stdin write, stdin EOF, terminate, and resize explicitly. PTY shells expose `resize_pty_blocking(...)` through the native manager. Adopted/stale records clear live-only controls even if the persisted record was created by a live owner.

Framework shutdown hooks are native. `shutdown_tree_blocking(root_pids)` terminates matching Ferrous-owned live shell roots, and `shutdown_tree_blocking(Vec::new())` means all Ferrous-owned live shell roots. `shutdown_all_blocking()` is the explicit all-live-roots alias. These hooks return the same `FerrousShutdownResult` DTO as group shutdown. Current native tree/all shutdown does not yet walk arbitrary procfs descendants outside Ferrous ownership.

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
- `POST /api/framework_shells/shutdown`
- `GET /api/framework_shells/logs/{shell_id}/tail`

Mutating routes require the same API token shape as Python FWS: `HMAC(secret, "api")`, passed as `X-Framework-Key` or `Authorization: Bearer ...`. `derive_native_api_token(...)` exposes that token derivation for Rust callers and tests.

The host also owns a Socket.IO controller lane for FWS peer interoperability:

- Socket.IO path: `/fws_ws/socket.io`
- Namespace: `/fws`
- Transport: websocket-only for the current MVP
- Peer event lane: `fws_peer_subscriptions`, `fws_peer_request`, and `fws_peer_notification`
- Browser/dashboard lane: `fws_request` and `fws_notification`

Peer auth uses the same shared-secret API token and runtime-id contract as Python FWS. Connected peers join the `fws:peers` room, receive active log-subscription hints, can return typed ack responses for `fws.shell.input`, and can push dashboard/log notifications back to the controller. The controller handles shell input local-first, then fans out to connected peers when local live input is unavailable. Group/tree/all shutdown currently terminates Ferrous-owned live shell roots and returns the shutdown DTO shape.

`FerrousNativePeer` is the matching Rust peer-client MVP. It connects to a Python or Ferrous controller using the same base URL plus `/fws_ws/socket.io`, authenticates as `role: "peer"`, tracks `fws_peer_subscriptions`, handles `fws_peer_request` for `fws.shell.input` by calling the local native manager write/EOF primitives, and returns the required Socket.IO ack DTO. It also exposes `emit_notification(...)` for explicit peer notifications. Automatic native event/log relay over this peer client is intentionally not claimed yet.

## FWS Environment Contract

`FerrousNativeManager::new()` derives the FWS child environment from the current process:

- `FRAMEWORK_SHELLS_SECRET`
- `FRAMEWORK_SHELLS_RUN_ID`
- `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`
- `TE_FRAMEWORK_URL`

If `FRAMEWORK_SHELLS_SECRET` is absent, Ferrous follows the FWS CLI bootstrap shape: it loads `runtimes/<repo_fingerprint>/secret` when present, otherwise generates a `temporary_secret_<hex>` value and stores it there with owner-only permissions where supported. If `FRAMEWORK_SHELLS_RUN_ID` is absent, Ferrous generates a native run id. URL values are optional until a host/dashboard runtime is attached.

When `FerrousNativeHost::spawn(...)` creates the manager, it sets `TE_FRAMEWORK_URL` and `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL` to the bound host URL when those values are otherwise absent. Child Ferrous/Python FWS peers use that URL with the fixed Socket.IO path `/fws_ws/socket.io` and namespace `/fws`.

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

Ferrous can also launch a rendered shellspec entry directly through `FerrousNativeManager::spawn_shellspec_entry_blocking(...)`. The API renders the selected entry, parses command/env/subgroups/backend/UI/debug metadata, and dispatches to native `proc`, `pipe`, or `pty`. Shellspec `command` may be a string array or a shell-style command string. Direct launch waits for supported readiness probes (`tcp_port`, `stdout_regex`) and rejects unsupported probe types explicitly.

For app-framework callers that need Python FWS app-worker metadata, use `spawn_shellspec_entry_with_overrides_blocking(...)` with `FerrousShellLaunchOverrides`. That explicit override surface carries `label`, `spec_id`, `subgroups`, UI/debug metadata, parent shell id, and caller env. TE2-style app workers should pass `label = app-worker:<app_id>`, `spec_id = app:<app_id>:<entry>`, and `subgroups = [app_id, "app-worker"]` so existing FWS discovery consumers can detect the app launch from metadata alone.

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
