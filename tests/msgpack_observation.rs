use ferrous_framework::msgpack_observation::decode_frame;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Fixtures {
    version: u32,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    hex: String,
    expected: Option<Value>,
    #[serde(default)]
    error: bool,
    #[serde(default)]
    incomplete: bool,
}
fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn fixtures_and_split_points() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("../testdata/msgpack_observation_cases.json")).unwrap();
    assert_eq!(fixtures.version, 1);
    for case in fixtures.cases {
        let data = bytes(&case.hex);
        let decoded = decode_frame(&data, 1024 * 1024);
        if case.error {
            assert!(decoded.is_err(), "{}", case.hex);
            continue;
        }
        let decoded = decoded.unwrap();
        if case.incomplete {
            assert!(decoded.is_none());
            continue;
        }
        let frame = decoded.unwrap();
        let expected = case.expected.unwrap();
        assert_eq!(
            json!({"value":frame.value,"consumed":frame.consumed}),
            expected,
            "{}",
            case.hex
        );
        for split in 0..frame.consumed {
            assert!(decode_frame(&data[..split], 1024 * 1024).unwrap().is_none());
            let combined = [&data[..split], &data[split..]].concat();
            let decoded = decode_frame(&combined, 1024 * 1024).unwrap().unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
        }
    }
}

#[test]
fn frame_budget() {
    assert!(
        decode_frame(&[0xdb, 0xff, 0xff, 0xff, 0xff], 1024 * 1024)
            .unwrap()
            .is_none()
    );
    assert!(decode_frame(&[0xc4, 0x20, b'x', b'x'], 4).is_err());
}
