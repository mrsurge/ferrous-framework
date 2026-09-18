use ferrous_framework::{
    log_projection::{Codec, WindowAction},
    log_window::{IndexCache, LineIndex, RESPONSE_BYTES},
};
use std::{fs, io::Write};

fn first_reference(
    index: &mut LineIndex,
    codec: Codec,
) -> ferrous_framework::log_projection::RawReference {
    index
        .window(WindowAction::Tail, 0, 10, 1, codec, RESPONSE_BYTES, None)
        .unwrap()
        .records[0]
        .raw
        .clone()
}

#[test]
fn cache_eviction_preserves_recent_index_and_raw_source() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0..3).map(|i| dir.path().join(i.to_string())).collect();
    for path in &paths {
        fs::write(path, b"first\n").unwrap();
    }
    let mut cache = IndexCache::new(2).unwrap();
    let reference = first_reference(
        cache.borrow(paths[0].clone(), Codec::Text).unwrap(),
        Codec::Text,
    );
    let evicted = first_reference(
        cache.borrow(paths[1].clone(), Codec::Text).unwrap(),
        Codec::Text,
    );
    assert_eq!(
        cache
            .borrow(paths[0].clone(), Codec::Text)
            .unwrap()
            .raw(&reference, 0, 64)
            .unwrap(),
        b"first\n"
    );
    cache.borrow(paths[2].clone(), Codec::Text).unwrap();
    assert_eq!(
        cache
            .borrow(paths[0].clone(), Codec::Text)
            .unwrap()
            .raw(&reference, 0, 64)
            .unwrap(),
        b"first\n"
    );
    assert!(
        cache
            .borrow(paths[1].clone(), Codec::Text)
            .unwrap()
            .raw(&evicted, 0, 64)
            .is_err()
    );
    drop(cache);
    assert_eq!(fs::read(&paths[0]).unwrap(), b"first\n");
    assert!(IndexCache::new(0).is_err());
}

#[test]
fn cache_reset_invalidates_all_codecs_without_file_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    fs::write(&path, b"{}\n").unwrap();
    let mut cache = IndexCache::new(2).unwrap();
    let text = first_reference(
        cache.borrow(path.clone(), Codec::Text).unwrap(),
        Codec::Text,
    );
    let json = first_reference(
        cache.borrow(path.clone(), Codec::Json).unwrap(),
        Codec::Json,
    );
    cache.invalidate(&path).unwrap();
    assert!(
        cache
            .borrow(path.clone(), Codec::Text)
            .unwrap()
            .raw(&text, 0, 64)
            .is_err()
    );
    assert!(
        cache
            .borrow(path, Codec::Json)
            .unwrap()
            .raw(&json, 0, 64)
            .is_err()
    );
}

#[test]
fn messagepack_partial_append_and_raw() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    let frame = [0x81, 0xa2, b'i', b'd', 7];
    fs::write(&path, [&frame[..], &frame[..2]].concat()).unwrap();
    let mut index = LineIndex::with_codec(path.clone(), Codec::Messagepack).unwrap();
    let window = index
        .window(
            WindowAction::Tail,
            0,
            1000,
            250,
            Codec::Messagepack,
            RESPONSE_BYTES,
            None,
        )
        .unwrap();
    assert_eq!(window.total, 1);
    assert_eq!(window.pending_bytes, 2);
    assert_eq!(window.records[0].text, "{\"id\":7}");
    assert_eq!(index.raw(&window.records[0].raw, 0, 65536).unwrap(), frame);
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&frame[2..])
        .unwrap();
    let next = index
        .window(
            WindowAction::Tail,
            0,
            1000,
            250,
            Codec::Messagepack,
            RESPONSE_BYTES,
            Some(&window.generation),
        )
        .unwrap();
    assert_eq!(next.total, 2);
    assert_eq!(next.pending_bytes, 0);
    assert_eq!(next.records[1].raw.byte_start, frame.len() as u64);
}

#[test]
fn corrupt_messagepack_never_becomes_partial_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    fs::write(&path, [0x81, 0xa2, b'i', b'd', 7, 0xc1]).unwrap();
    let mut index = LineIndex::with_codec(path, Codec::Messagepack).unwrap();
    for _ in 0..2 {
        assert!(
            index
                .window(
                    WindowAction::Tail,
                    0,
                    1000,
                    250,
                    Codec::Messagepack,
                    RESPONSE_BYTES,
                    None
                )
                .is_err()
        );
    }
}

#[test]
fn append_partial_raw_and_reset() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    fs::write(&path, b"one\r\ntwo\npart").unwrap();
    let mut index = LineIndex::new(path.clone()).unwrap();
    let first = index
        .window(
            WindowAction::Tail,
            0,
            2,
            1,
            Codec::Text,
            RESPONSE_BYTES,
            None,
        )
        .unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>(),
        vec!["two\n", "part"]
    );
    assert_eq!((first.start, first.end, first.total), (1, 3, 3));
    let reference = first.records[1].raw.clone();
    assert_eq!(index.raw(&reference, 1, 2).unwrap(), b"ar");
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"ial\nlast\n")
        .unwrap();
    let next = index
        .window(
            WindowAction::Tail,
            0,
            2,
            1,
            Codec::Text,
            RESPONSE_BYTES,
            Some(&first.generation),
        )
        .unwrap();
    assert_eq!(
        next.records
            .iter()
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>(),
        vec!["partial\n", "last\n"]
    );
    assert_eq!(next.records[0].raw.byte_start, reference.byte_start);
    let older = index
        .window(
            WindowAction::Older,
            next.start,
            2,
            1,
            Codec::Text,
            RESPONSE_BYTES,
            None,
        )
        .unwrap();
    assert_eq!(
        older
            .records
            .iter()
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>(),
        vec!["two\n"]
    );
    fs::write(&path, b"new\n").unwrap();
    assert!(index.raw(&reference, 0, 10).is_err());
}

#[test]
fn response_budget_and_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    fs::write(&path, b"").unwrap();
    let mut index = LineIndex::new(path.clone()).unwrap();
    let empty = index
        .window(WindowAction::Tail, 0, 1000, 250, Codec::Text, 1024, None)
        .unwrap();
    assert_eq!(empty.total, 0);
    assert!(empty.at_start && empty.at_tail);
    fs::write(&path, b"line\n".repeat(1000)).unwrap();
    let tail = index
        .window(WindowAction::Tail, 0, 1000, 250, Codec::Text, 1024, None)
        .unwrap();
    assert!(serde_json::to_vec(&tail).unwrap().len() <= 1024);
    assert_eq!(tail.end, 1000);
    assert!(tail.start > 0);
    let current = index
        .window(WindowAction::Current, 0, 1000, 250, Codec::Text, 1024, None)
        .unwrap();
    assert_eq!(current.start, 0);
    assert!(current.end < 1000);
}

#[test]
fn giant_line_and_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stdout");
    let mut original = vec![b'x'; 2 * 1024 * 1024];
    original.push(b'\n');
    fs::write(&path, &original).unwrap();
    let mut index = LineIndex::new(path.clone()).unwrap();
    let window = index
        .window(
            WindowAction::Tail,
            0,
            1000,
            250,
            Codec::Json,
            RESPONSE_BYTES,
            None,
        )
        .unwrap();
    let record = &window.records[0];
    assert_eq!(record.diagnostic.as_deref(), Some("parse_budget_exceeded"));
    assert!(record.text.len() <= 8192);
    assert_eq!(
        index
            .raw(&record.raw, original.len() as u64 - 3, 65536)
            .unwrap(),
        b"xx\n"
    );
    assert_eq!(fs::read(&path).unwrap(), original);
    let replacement = path.with_extension("new");
    fs::write(&replacement, b"replacement").unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(
        index
            .window(
                WindowAction::Tail,
                0,
                1000,
                250,
                Codec::Text,
                RESPONSE_BYTES,
                Some(&window.generation)
            )
            .is_err()
    );
}

#[test]
fn byte_limited_navigation_has_no_gaps() {
    for codec in [Codec::Text, Codec::Messagepack] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stdout");
        let frame: &[u8] = if matches!(codec, Codec::Text) {
            b"line\n"
        } else {
            &[0x81, 0xa2, b'i', b'd', 7]
        };
        fs::write(&path, frame.repeat(40)).unwrap();
        let mut index = LineIndex::with_codec(path, codec).unwrap();
        let mut page = index
            .window(WindowAction::Tail, 0, 1000, 250, codec, 1024, None)
            .unwrap();
        let current = index
            .window(
                WindowAction::Current,
                page.start,
                1000,
                250,
                codec,
                1024,
                None,
            )
            .unwrap();
        assert_eq!((current.start, current.end), (page.start, page.end));
        let mut seen = page.records.len();
        while !page.at_start {
            let older = index
                .window(
                    WindowAction::Older,
                    page.start,
                    1000,
                    250,
                    codec,
                    1024,
                    None,
                )
                .unwrap();
            assert_eq!(older.end, page.start);
            assert!(older.start < page.start);
            seen += older.records.len();
            page = older;
        }
        assert_eq!(seen, 40);
        seen = page.records.len();
        while !page.at_tail {
            let newer = index
                .window(WindowAction::Newer, page.end, 1000, 250, codec, 1024, None)
                .unwrap();
            assert_eq!(newer.start, page.end);
            assert!(newer.end > page.end);
            seen += newer.records.len();
            page = newer;
        }
        assert_eq!(seen, 40);
    }
}
