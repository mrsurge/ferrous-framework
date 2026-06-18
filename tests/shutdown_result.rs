use ferrous_framework::shutdown::parse_shutdown_result;
use serde_json::json;

#[test]
fn parses_shutdown_result_dto() {
    let value = json!({
        "ok": true,
        "kind": "shutdown_group",
        "target": "demo",
        "started_at_ms": 1000,
        "ended_at_ms": 1250,
        "elapsed_ms": 250,
        "root_pids": [101, 202],
        "stats": {
            "total": 2,
            "terminated": 2,
            "clean_exits": 1,
            "force_killed": 1,
            "errors": ["forced"]
        },
        "events": ["terminate shell fs_1", "sigkill shell fs_2"],
        "note": "ok"
    });
    let result = parse_shutdown_result(&value).expect("shutdown result");
    assert!(result.ok);
    assert_eq!(result.kind, "shutdown_group");
    assert_eq!(result.target, "demo");
    assert_eq!(result.elapsed_ms, 250);
    assert_eq!(result.root_pids, vec![101, 202]);
    assert_eq!(result.stats.total, 2);
    assert_eq!(result.stats.force_killed, 1);
    assert_eq!(result.stats.errors, vec!["forced"]);
    assert_eq!(result.events.len(), 2);
    assert_eq!(result.note.as_deref(), Some("ok"));
}

#[test]
fn rejects_missing_stats() {
    let value = json!({
        "ok": true,
        "kind": "shutdown_group",
        "target": "demo",
        "started_at_ms": 1000,
        "ended_at_ms": 1250,
        "elapsed_ms": 250,
        "root_pids": [],
        "events": []
    });
    let error = parse_shutdown_result(&value).expect_err("missing stats must fail");
    assert!(error.to_string().contains("missing stats"));
}
