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
- `FerrousNativeProcConfig`
- `FerrousNativePipeConfig`
- `FerrousNativePtyConfig`
- `FerrousNativeShellRecord`
- `FerrousNativeShellStatus`
- `FerrousNativeShellCapabilities`

## Native Backend Coverage

```text
proc: launch, stdout/stderr logs, list/get, terminate, wait
pipe: launch, direct stdin writes, direct stdout reads, stdout/stderr logs, list/get, terminate, wait
pty: launch, direct PTY writes, direct PTY reads, PTY output log, list/get, terminate, wait
```

The `pipe` and `pty` hot paths are direct fd paths. They do not use a Python bridge, a stdout pump queue, or a drain worker. Reads are caller-driven and tee output to the log as bytes are read.

## Example

```rust
use ferrous_framework::{FerrousNativeManager, FerrousNativePipeConfig};
use std::{collections::HashMap, path::PathBuf, time::Duration};

let manager = FerrousNativeManager::new();
let shell = manager.spawn_pipe_blocking(FerrousNativePipeConfig {
    command: vec!["sh".into(), "-c".into(), "while read line; do echo ack:$line; done".into()],
    cwd: None,
    env: HashMap::new(),
    label: "worker".into(),
    spec_id: "worker".into(),
    subgroups: vec!["demo".into()],
    log_dir: PathBuf::from("target/fws-logs"),
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

## Test/Bench Signals

The native pipe tests include JSON-RPC-shaped request/response coverage with interleaved notifications and visible timing output when run with `-- --nocapture`:

```sh
cargo test pipe_ -- --nocapture
```
