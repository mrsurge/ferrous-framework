# ferrous-framework

`ferrous-framework` is the Rust implementation of FWS, the process-runtime contract also implemented by Python [`framework-shells`](https://github.com/mrsurge/framework-shells).

Use it when a Rust application needs to start child processes and keep them observable without putting Python on the hot path for process I/O.

Ferrous gives Rust hosts one consistent way to:

- launch child processes
- stop them cleanly
- group them by app/project/purpose
- capture stdout/stderr logs
- write to stdin when the backend supports it
- expose FWS-compatible shell records
- serve the FWS dashboard/control API
- interoperate with Python FWS managers over the peer lane
- keep runtime behavior configurable through shellspecs

Examples of things Ferrous can supervise:

- Rust app workers
- language servers
- build workers
- JSON-RPC stdio services
- file-system or git helpers
- terminal-facing PTY processes
- adapter processes
- nested FWS-compatible child managers

Ferrous is not the application protocol. It does not know what your JSON-RPC methods mean, how your DTOs are shaped, or how your business logic routes requests. It owns process lifecycle and observability. Your application owns protocol semantics.

Use Python `framework-shells` when the host runtime is Python or when FastAPI/ASGI mounting is the natural integration point. Use `ferrous-framework` when the host runtime is Rust, when process I/O must stay native, or when a compiled framework needs the FWS runtime contract.

## Mental Model

Ferrous is a Rust process manager plus an FWS-compatible metadata/log store.

```text
Rust application
  -> ferrous-framework crate
  -> Rust-owned proc/pipe/pty runtime
  -> FWS-compatible records, logs, capabilities, dashboard, and peer state
```

When Ferrous starts a child process, it creates a shell record. The record answers the same questions as a Python FWS record:

- what command was launched?
- what backend owns it?
- what PID is currently running?
- where are stdout/stderr logs?
- what labels and groups does it belong to?
- can this manager write to stdin?
- can this manager stream output live?
- is the process still running or has it exited?

Ferrous uses the same default store shape as Python FWS:

```text
~/.cache/framework_shells/runtimes/<repo_fingerprint>/<runtime_id>/
  meta/<shell_id>/meta.json
  logs/<shell_id>.stdout.log
  logs/<shell_id>.stderr.log
  sockets/
```

The runtime is derived from the FWS secret. That is what lets Python FWS and Ferrous managers share records and peer-control semantics without becoming the same implementation.

## Backends

Ferrous keeps the backend model intentionally small.

| Backend | Use it for | Shape |
| --- | --- | --- |
| `proc` | app workers, services, build jobs, helpers that do not need stdin | supervised process plus stdout/stderr logs |
| `pipe` | JSON-RPC stdio servers, protocol adapters, language workers, structured backend tools | supervised stdin/stdout/stderr byte streams |
| `pty` | interactive shells, terminal applications, TUI-like processes | supervised terminal byte stream with input and resize |

The `pipe` backend is protocol-neutral. Ferrous does not parse JSON-RPC, line protocols, editor control messages, or application DTOs. It owns the child process, stdin writes, stdout/stderr capture, logs, capabilities, and shutdown. The consumer owns framing and semantics.

`pipe` and `pty` stdout are direct fd paths. They do not use Python, a broker process, or a stdout pump queue. Reads are caller-driven and tee output to logs as bytes are drained.

A manager-owned reactor handles passive proc stdout/stderr logging, pipe stderr logging, and child-exit persistence. Pipe and PTY stdout stay on the direct read path so protocol readers keep ownership of the bytes.

PTY launch supports `FerrousNativePtyMode::Raw` and `FerrousNativePtyMode::Interactive`. Raw mode applies raw termios to the PTY slave before spawning the child. Shellspec `pty_mode` is honored by native launch.

## Shellspecs

A shellspec is a YAML launch file.

It is not a new protocol. It is a declarative way to say: "these are the runtime processes this app may need, and here is how to launch them."

Ferrous can render and launch shellspec entries natively. Shellspec compatibility matters because a compiled Rust framework should be able to change runtime parameters without rebuilding the binary.

A shellspec can describe:

- shell id inside the spec
- backend (`proc`, `pipe`, or `pty`)
- command and arguments
- working directory
- environment variables
- labels and subgroups
- readiness checks
- dashboard/UI hints
- debug metadata

Example:

```yaml
version: "1"
shells:
  rpc_worker:
    backend: pipe
    cwd: ${ctx:PROJECT_ROOT}
    command: ["node", "dist/app-server.mjs"]
    env:
      APP_ID: ${ctx:APP_ID}
      PORT: ${free_port}
    subgroups: ["demo", "rpc"]
    inspect_hints:
      - json
      - jsonrpc
```

In that example:

- `rpc_worker` is the shellspec entry id.
- `${ctx:PROJECT_ROOT}` and `${ctx:APP_ID}` come from the caller-supplied render context.
- `${free_port}` asks the renderer to reserve and reuse one free port during rendering.
- Ferrous launches the rendered `pipe` worker and writes FWS-compatible metadata/log records.

Primary shellspec entry points:

- `shellspec::render_shellspec_entry(...)`
- `FerrousNativeManager::spawn_shellspec_entry_blocking(...)`
- `FerrousNativeManager::spawn_shellspec_entry_with_overrides_blocking(...)`
- `FerrousNativeManager::apply_shellspec_document_blocking(...)`

For app-framework callers that need Python FWS app-worker discovery semantics, use `spawn_shellspec_entry_with_overrides_blocking(...)` with `FerrousShellLaunchOverrides`. TE2-style app workers should pass:

```text
label = app-worker:<app_id>
spec_id = app:<app_id>:<entry>
subgroups = [app_id, "app-worker"]
```

That keeps existing FWS discovery consumers able to detect app launches from metadata alone.

## Quick Rust Usage

```rust
use ferrous_framework::{FerrousNativeManager, FerrousNativePipeConfig};
use std::{collections::HashMap, time::Duration};

let manager = FerrousNativeManager::new();

let shell = manager.spawn_pipe_blocking(FerrousNativePipeConfig {
    command: vec![
        "sh".into(),
        "-c".into(),
        "while read line; do echo ack:$line; done".into(),
    ],
    cwd: None,
    env: HashMap::new(),
    label: "rpc-worker".into(),
    spec_id: "rpc-worker".into(),
    subgroups: vec!["demo".into(), "rpc".into()],
    log_dir: None,
})?;

manager.write_line_blocking(&shell.id, r#"{"jsonrpc":"2.0","id":1}"#)?;
let response = manager.read_line_blocking(&shell.id, Duration::from_secs(5))?;
```

For normal services use `spawn_proc_blocking(...)`. For terminal-like processes use `spawn_pty_blocking(...)` and `resize_pty_blocking(...)`.

## Native Host And Dashboard

`FerrousNativeHost` serves a Rust-owned FWS host around `FerrousNativeManager`.

It provides:

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

Mutating routes use the same API token shape as Python FWS: `HMAC(secret, "api")`, passed as `X-Framework-Key` or `Authorization: Bearer ...`. Rust callers can derive that token with `derive_native_api_token(...)`.

The dashboard assets are the same FWS dashboard product surface, served by the Rust host. The dashboard is an observability/control surface. It is not required for supervised processes to run.

## Peer Interoperability

Ferrous and Python FWS can run as separate managers and still cooperate through the FWS peer lane.

The current Socket.IO binding is:

- path: `/fws_ws/socket.io`
- namespace: `/fws`
- peer events: `fws_peer_subscriptions`, `fws_peer_request`, `fws_peer_notification`
- dashboard events: `fws_request`, `fws_notification`

`FerrousNativeHost` acts as a controller. It accepts authenticated peers, tracks log-subscription hints, receives lifecycle/log notifications, and routes shell input local-first before fanning out to peers when local live input is unavailable.

`FerrousNativePeer` is the Rust peer client. It can connect to either a Ferrous controller or a Python FWS controller. It handles `fws.shell.input` peer requests by calling local native stdin/EOF primitives and returns the expected ack DTO. It also forwards lifecycle events and subscribed output chunks without independently draining direct pipe/PTY stdout.

The target is interoperability, not a Ferrous-only protocol. Python FWS and Ferrous should be able to share metadata, dashboard semantics, shellspec conventions, and peer lanes while keeping their own runtime implementations.

## Environment Contract

Ferrous understands the same FWS environment keys used by Python FWS:

| Variable | Meaning |
| --- | --- |
| `FRAMEWORK_SHELLS_SECRET` | Runtime secret and API auth root |
| `FRAMEWORK_SHELLS_RUN_ID` | Current manager run id |
| `FRAMEWORK_SHELLS_BASE_DIR` | Runtime store base directory |
| `FRAMEWORK_SHELLS_REPO_FINGERPRINT` | Runtime fingerprint override |
| `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL` | Parent/controller FWS URL for peer connection |
| `TE_FRAMEWORK_URL` | Host framework URL used by FWS-compatible children |

`FerrousNativeManager::new()` derives missing values from the current process and FWS store. If `FRAMEWORK_SHELLS_SECRET` is absent, Ferrous loads `runtimes/<repo_fingerprint>/secret` when present; otherwise it generates a `temporary_secret_<hex>` value and stores it with owner-only permissions where supported.

`FerrousNativeHost::spawn(...)` sets `TE_FRAMEWORK_URL` and `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL` to the bound host URL when those values are absent. Child Ferrous or Python FWS managers use that URL with the fixed Socket.IO path and namespace.

Use `FerrousNativeManager::with_env(...)`, `try_with_env_map(...)`, or `with_env_map(...)` when the host already resolved the FWS environment explicitly. Native spawn config env values are applied after the manager overlay, so per-shell overrides still win.

## Compatibility Surfaces

Ferrous is native Rust, but it keeps a few compatibility-shaped APIs for downstream consumers that migrated from the old Python/PyO3 bridge.

`FerrousNativeManager` exposes Python-FWS-shaped async names over the native runtime:

- `spawn_shell(...)`
- `spawn_shell_pipe(...)`
- `spawn_shell_pty(...)`
- `get_pipe_state(...)`
- `write_to_pipe(...)`
- `write_to_shell(...)`
- `send_shell_eof(...)`
- `terminate_shell(...)`
- `subscribe_output_bytes(...)`

These names do not add JSON-RPC framing, app protocol routing, Python networking, or Python I/O. They are compatibility names over native Rust primitives.

`FerrousFrameworkPipe::spawn(FerrousPipeConfig { ... })` provides the ALS-style blocking pipe adapter:

- `shell_id()`
- `write_line_blocking(...)`
- `read_line_blocking()`
- `close_blocking()`

That wrapper is intentionally pipe-only. If a shellspec renders to a non-pipe backend, it errors instead of pretending line-oriented pipe semantics are available.

`pyo3_embed_enabled()` is retained as a legacy availability gate for older callers. In the native crate it does not mean Python is in the runtime path. `ferrous_native_enabled()` is the literal native-runtime capability flag.

## Shutdown

Ferrous shutdown hooks are native and return `FerrousShutdownResult` / `FerrousShutdownStats` DTOs.

Important calls:

- `shutdown_app_group_blocking(app_id)` selects running records whose derived app id matches and shuts down those roots plus procfs descendants.
- `shutdown_tree_blocking(root_pids)` shuts down the requested root PIDs plus procfs descendants.
- `shutdown_tree_blocking(Vec::new())` selects all Ferrous-owned live root PIDs and shuts down those roots plus descendants.
- `shutdown_all_blocking()` is the explicit all-live-roots alias.

The implementation protects the current Ferrous process ancestry before signaling. It sends SIGTERM first, waits briefly, then sends SIGKILL to survivors. Persisted FWS records whose PIDs are affected by the shutdown are marked exited so mixed Python/Ferrous environments do not keep stale running metadata.

## Output, Logs, And Metadata

Raw stdout/stderr logs stay raw. Ferrous writes FWS-compatible metadata and log paths so Python FWS, Ferrous, dashboards, and inspection tools can agree on process state.

Native records include:

- command/backend/status
- pid and exit code
- stdout/stderr log paths
- capabilities
- labels and subgroups
- UI/debug metadata
- explicit launch `env_overrides`
- env keys without secret values
- runtime id and app metadata

Fresh managers can load persisted records from the canonical metadata directory. Loaded records are marked `adopted: true`, keep log metadata, and clear live-only controls such as stdin write when the current manager does not own the live process.

`io_metadata_log` is currently a stable sidecar path declaration. It is not proof that Ferrous is writing I/O metadata sidecar records yet.

### Bounded Views And Stream Codecs

The shared FWS dashboard reads bounded source-line windows, with Older/Newer/Live
tail navigation. Existing live events trigger window reads without polling.
Paused/history views do not accumulate incoming output; raw log files remain
unchanged. Original bytes are available in pages of at most 64 KiB.

Shellspecs can declare `log_codecs: {stdout: messagepack, stderr: text}`.
The supported values are `text` (default), `json`, and `messagepack`, with normal
ctx/env template rendering. MessagePack observation indexes concatenated complete
objects. It does not change child stdio or Socket.IO serialization. Codec metadata
is persisted and shared with Python FWS, whose `fws inspect` decodes it too.

Native manager APIs are `log_window_blocking` and `log_raw_blocking`; hosts expose
`/api/framework_shells/logs/{shell_id}/window` and `/raw` on blocking I/O executors.
See [the projection plan](docs/LOG_PROJECTION_PLAN.md) for budgets and remaining
acceptance work, including display-window filtering and historical ANSI state.

## Tests And Benchmarks

Default correctness checks:

```sh
cargo test
```

Pipe timing output:

```sh
cargo test pipe_ -- --nocapture
```

PTY terminal timing output:

```sh
cargo test pty_terminal -- --nocapture
```

Opt-in async facade performance probes:

```sh
cargo test --release pipe_async_facade_reports_rtt_overhead_against_blocking_direct -- --ignored --nocapture
cargo test --release pipe_async_facade_reports_concurrent_inflight_metrics -- --ignored --nocapture
```

Shellspec parity fixtures live under `testdata/` and run as part of the normal test suite.
