use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

fn coerce_usize(value: Option<&Value>, default: usize) -> usize {
    value
        .and_then(Value::as_u64)
        .and_then(|raw| usize::try_from(raw).ok())
        .unwrap_or(default)
}

fn payload(size: usize, fill: char) -> String {
    std::iter::repeat(fill).take(size).collect()
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<Value>(&line) {
            Ok(Value::Object(object)) => Value::Object(object),
            _ => {
                writeln!(
                    stdout,
                    "{}",
                    json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}})
                )?;
                stdout.flush()?;
                continue;
            }
        };
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let params = request.get("params").and_then(Value::as_object);
        let push_count = coerce_usize(params.and_then(|p| p.get("push_count")), 0);
        let payload_size = coerce_usize(params.and_then(|p| p.get("payload_size")), 0);
        let echo = params
            .and_then(|p| p.get("echo"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        for index in 0..push_count {
            writeln!(
                stdout,
                "{}",
                json!({
                    "jsonrpc":"2.0",
                    "method":"bench.push",
                    "params":{"id":id,"index":index,"payload":payload(payload_size, 'p')}
                })
            )?;
        }
        writeln!(
            stdout,
            "{}",
            json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{"ok":true,"echo":echo,"payload":payload(payload_size, 'r')}
            })
        )?;
        stdout.flush()?;
    }
    Ok(())
}
