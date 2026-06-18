use ferrous_framework::shellspec::{ShellspecRenderInput, render_shellspec_value};
use serde_json::{Map, Number, Value};
use std::collections::HashMap;

const FIXTURE: &str = include_str!("../testdata/shellspec_parity_cases.json");

fn object(value: &Value) -> &Map<String, Value> {
    value.as_object().expect("expected object")
}

fn array(value: &Value) -> &[Value] {
    value.as_array().expect("expected array")
}

fn string_map(value: &Value) -> HashMap<String, String> {
    object(value)
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
        .collect()
}

fn normalize_rendered_shell(case: &Value) -> Value {
    let case_obj = object(case);
    let input = ShellspecRenderInput {
        ctx: string_map(&case_obj["ctx"]),
        env: string_map(&case_obj["env"]),
    };
    let rendered_doc =
        render_shellspec_value(&case_obj["document"], &input).expect("rendered document");
    let entry = case_obj["entry"].as_str().expect("entry string");
    let shells = object(&object(&rendered_doc)["shells"]);
    let shell = object(&shells[entry]);

    let mut out = Map::new();
    out.insert(
        "id".to_owned(),
        shell
            .get("id")
            .cloned()
            .unwrap_or_else(|| Value::String(entry.to_owned())),
    );
    out.insert(
        "backend".to_owned(),
        shell
            .get("backend")
            .cloned()
            .unwrap_or_else(|| Value::String("proc".to_owned())),
    );
    if let Some(value) = shell.get("pty_mode") {
        out.insert("pty_mode".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("cwd") {
        out.insert("cwd".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("command") {
        out.insert("command".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("env") {
        out.insert("env".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("subgroups") {
        out.insert("subgroups".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("pipe") {
        out.insert("pipe".to_owned(), value.clone());
    }
    if let Some(value) = shell.get("readiness") {
        let mut readiness = object(value).clone();
        if let Some(port) = readiness.get("port").and_then(Value::as_str) {
            if let Ok(port_num) = port.parse::<u64>() {
                readiness.insert("port".to_owned(), Value::Number(Number::from(port_num)));
            }
        }
        if let Some(timeout) = readiness.get("timeout").and_then(Value::as_str) {
            if let Ok(timeout_num) = timeout.parse::<f64>() {
                if let Some(number) = Number::from_f64(timeout_num) {
                    readiness.insert("timeout".to_owned(), Value::Number(number));
                }
            }
        }
        readiness
            .entry("status_codes".to_owned())
            .or_insert_with(|| Value::Array(vec![Value::Number(Number::from(200_u64))]));
        out.insert("readiness".to_owned(), Value::Object(readiness));
    }
    out.insert(
        "autostart".to_owned(),
        shell.get("autostart").cloned().unwrap_or(Value::Bool(true)),
    );
    Value::Object(out)
}

fn assert_matches_expected(
    actual: &Value,
    expected: &Value,
    marker: &str,
    free_ports: &mut Vec<Value>,
) {
    if expected.as_str() == Some(marker) {
        free_ports.push(actual.clone());
        return;
    }
    match expected {
        Value::Object(expected_obj) => {
            let actual_obj = object(actual);
            for (key, expected_value) in expected_obj {
                assert!(actual_obj.contains_key(key), "missing key {key}");
                assert_matches_expected(&actual_obj[key], expected_value, marker, free_ports);
            }
        }
        Value::Array(expected_items) => {
            let actual_items = array(actual);
            assert_eq!(actual_items.len(), expected_items.len());
            for (actual_item, expected_item) in actual_items.iter().zip(expected_items) {
                assert_matches_expected(actual_item, expected_item, marker, free_ports);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}

fn assert_free_ports(values: &[Value]) {
    if values.is_empty() {
        return;
    }
    let ports = values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
                .expect("free port value")
        })
        .collect::<Vec<_>>();
    let first = ports[0];
    for port in ports {
        assert_eq!(port, first);
        assert!((1..=65535).contains(&port));
    }
}

#[test]
fn renders_shellspec_parity_fixtures() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture json");
    let marker = object(&fixture)["free_port_marker"]
        .as_str()
        .expect("marker string");
    for case in array(&object(&fixture)["cases"]) {
        let actual = normalize_rendered_shell(case);
        let expected = &object(case)["expect"];
        let mut free_ports = Vec::new();
        assert_matches_expected(&actual, expected, marker, &mut free_ports);
        assert_free_ports(&free_ports);
    }
}
