use ferrous_framework::log_projection::{
    Codec, RawReference, WindowAction, project_record, window_start,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Fixtures {
    version: u32,
    records: Vec<RecordCase>,
    windows: Vec<WindowCase>,
}
#[derive(Deserialize)]
struct RecordCase {
    source: String,
    codec: Codec,
    budget: usize,
    expected: Value,
}
#[derive(Deserialize)]
struct WindowCase {
    total: usize,
    current: usize,
    count: usize,
    shift: usize,
    action: WindowAction,
    expected: usize,
}

#[test]
fn shared_fixtures() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("../testdata/log_projection_cases.json")).unwrap();
    assert_eq!(fixtures.version, 1);
    for mut case in fixtures.records {
        let data = case.source.as_bytes();
        let raw = RawReference {
            generation: "fixture".into(),
            byte_start: 17,
            byte_end: 17 + data.len() as u64,
        };
        let result = project_record(data, raw, case.codec, case.budget, 1024 * 1024).unwrap();
        assert!(result.text.len() <= case.budget);
        case.expected["raw"] =
            json!({"generation":"fixture","byte_start":17,"byte_end":17 + data.len()});
        assert_eq!(serde_json::to_value(result).unwrap(), case.expected);
    }
    for case in fixtures.windows {
        assert_eq!(
            window_start(
                case.total,
                case.current,
                case.count,
                case.shift,
                case.action
            )
            .unwrap(),
            case.expected
        );
    }
}

#[test]
fn validation_and_parser_ceiling() {
    assert!(window_start(10, 0, 0, 1, WindowAction::Tail).is_err());
    assert!(
        project_record(
            b"x",
            RawReference {
                generation: "x".into(),
                byte_start: 0,
                byte_end: 2
            },
            Codec::Text,
            32,
            64
        )
        .is_err()
    );
    let data = format!("{{\"huge\":\"{}\"}}", "x".repeat(1000));
    let result = project_record(
        data.as_bytes(),
        RawReference {
            generation: "x".into(),
            byte_start: 0,
            byte_end: data.len() as u64,
        },
        Codec::Json,
        32,
        64,
    )
    .unwrap();
    assert_eq!(result.diagnostic.as_deref(), Some("parse_budget_exceeded"));
}
