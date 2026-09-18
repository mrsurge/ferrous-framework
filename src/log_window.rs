//! Synchronous indexed reads; async hosts must use their blocking I/O executor.
use crate::log_projection::{
    Codec, PARSE_BYTES, RECORD_BYTES, RawReference, RecordProjection, WindowAction, project_record,
};
use anyhow::{Result, bail, ensure};
use serde::Serialize;
use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

pub const SCAN_BYTES: usize = 65536;
pub const RESPONSE_BYTES: usize = 1024 * 1024;

pub fn reset_path(path: &std::path::Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".fws-reset");
    PathBuf::from(name)
}

fn reset_token(path: &std::path::Path) -> Result<Vec<u8>> {
    let marker = reset_path(path);
    match File::open(marker) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(64).read_to_end(&mut bytes)?;
            Ok(bytes)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

pub fn mark_log_reset(path: &std::path::Path) -> Result<()> {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|e| anyhow::anyhow!("{e}"))?;
    let token: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
    let mut temporary =
        tempfile::NamedTempFile::new_in(path.parent().unwrap_or(std::path::Path::new(".")))?;
    temporary.write_all(token.as_bytes())?;
    temporary.persist(reset_path(path))?;
    Ok(())
}

pub fn validate_log_codecs(
    value: Option<&serde_json::Value>,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(Default::default());
    };
    let map = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("log_codecs must be a mapping"))?;
    for (stream, codec) in map {
        ensure!(
            matches!(stream.as_str(), "stdout" | "stderr"),
            "log_codecs keys must be stdout or stderr"
        );
        ensure!(
            matches!(codec.as_str(), Some("text" | "json" | "messagepack")),
            "log codec must be text, json, or messagepack"
        );
    }
    Ok(map.clone())
}

pub fn stream_codec(
    codecs: &serde_json::Map<String, serde_json::Value>,
    stream: &str,
) -> Result<Codec> {
    ensure!(
        matches!(stream, "stdout" | "stderr"),
        "stream must be stdout or stderr"
    );
    let codec = match codecs.get(stream) {
        None => "text",
        Some(value) => value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid log codec"))?,
    };
    match codec {
        "text" => Ok(Codec::Text),
        "json" => Ok(Codec::Json),
        "messagepack" => Ok(Codec::Messagepack),
        _ => bail!("invalid log codec"),
    }
}

/// Bounded index ownership. Use on a blocking executor behind the owner's mutex.
/// Paths must come from authorized shell records, never arbitrary client paths.
pub struct IndexCache {
    capacity: usize,
    entries: VecDeque<(PathBuf, Codec, LineIndex)>,
}

impl IndexCache {
    pub fn new(capacity: usize) -> Result<Self> {
        ensure!(capacity > 0, "index capacity must be positive");
        Ok(Self {
            capacity,
            entries: VecDeque::new(),
        })
    }

    pub fn borrow(&mut self, path: PathBuf, codec: Codec) -> Result<&mut LineIndex> {
        let path = std::path::absolute(path)?;
        if let Some(position) = self
            .entries
            .iter()
            .position(|(p, c, _)| *p == path && *c == codec)
        {
            let entry = self.entries.remove(position).expect("located cache entry");
            self.entries.push_back(entry);
        } else {
            if self.entries.len() >= self.capacity {
                self.entries.pop_front();
            }
            let index = LineIndex::with_codec(path.clone(), codec)?;
            self.entries.push_back((path, codec, index));
        }
        Ok(&mut self.entries.back_mut().expect("inserted cache entry").2)
    }

    pub fn invalidate(&mut self, path: &std::path::Path) -> Result<()> {
        let path = std::path::absolute(path)?;
        for (source, _, index) in &mut self.entries {
            if *source == path {
                index.invalidate();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct LogWindow {
    pub generation: String,
    pub start: usize,
    pub end: usize,
    pub total: usize,
    pub at_start: bool,
    pub at_tail: bool,
    pub records: Vec<RecordProjection>,
    pub pending_bytes: u64,
}

pub struct LineIndex {
    path: PathBuf,
    index: File,
    identity: Option<(u64, u64)>,
    mtime: (i64, i64),
    size: u64,
    complete: usize,
    last_end: u64,
    generation: String,
    codec: Codec,
    pending: Vec<u8>,
    reset: Vec<u8>,
}

impl LineIndex {
    pub fn new(path: PathBuf) -> Result<Self> {
        Self::with_codec(path, Codec::Text)
    }
    pub fn with_codec(path: PathBuf, codec: Codec) -> Result<Self> {
        Ok(Self {
            codec,
            pending: Vec::new(),
            reset: Vec::new(),
            path,
            index: tempfile::tempfile()?,
            identity: None,
            mtime: (0, 0),
            size: 0,
            complete: 0,
            last_end: 0,
            generation: String::new(),
        })
    }
    pub fn invalidate(&mut self) {
        self.identity = None;
    }
    fn refresh(&mut self) -> Result<()> {
        let result = self.refresh_inner();
        if result.is_err() {
            self.identity = None;
        }
        result
    }
    fn refresh_inner(&mut self) -> Result<()> {
        let mut source = File::open(&self.path)?;
        let stat = source.metadata()?;
        let identity = (stat.dev(), stat.ino());
        let mtime = (stat.mtime(), stat.mtime_nsec());
        let token = reset_token(&self.path)?;
        if self.reset != token
            || self.identity != Some(identity)
            || stat.len() < self.size
            || (stat.len() == self.size && mtime != self.mtime)
        {
            self.index.set_len(0)?;
            self.size = 0;
            self.complete = 0;
            self.last_end = 0;
            self.pending.clear();
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(|e| anyhow::anyhow!("{e}"))?;
            self.generation = nonce.iter().map(|b| format!("{b:02x}")).collect();
        }
        self.identity = Some(identity);
        self.reset = token;
        source.seek(SeekFrom::Start(self.size))?;
        self.index.seek(SeekFrom::End(0))?;
        let mut buffer = [0u8; SCAN_BYTES];
        while self.size < stat.len() {
            let take = (stat.len() - self.size).min(SCAN_BYTES as u64) as usize;
            let n = source.read(&mut buffer[..take])?;
            ensure!(n > 0, "log changed during indexing");
            let mut offsets = Vec::new();
            if matches!(self.codec, Codec::Messagepack) {
                self.pending.extend_from_slice(&buffer[..n]);
                let mut consumed = 0;
                while consumed < self.pending.len() {
                    let Some(frame) = crate::msgpack_observation::decode_frame(
                        &self.pending[consumed..],
                        PARSE_BYTES,
                    )?
                    else {
                        break;
                    };
                    consumed += frame.consumed;
                    self.last_end += frame.consumed as u64;
                    offsets.extend_from_slice(&self.last_end.to_le_bytes());
                    self.complete += 1;
                }
                self.pending.drain(..consumed);
            } else {
                for (position, byte) in buffer[..n].iter().enumerate() {
                    if *byte == b'\n' {
                        self.last_end = self.size + position as u64 + 1;
                        offsets.extend_from_slice(&self.last_end.to_le_bytes());
                        self.complete += 1;
                    }
                }
            }
            self.index.write_all(&offsets)?;
            self.size += n as u64;
        }
        self.mtime = mtime;
        self.validate_snapshot()?;
        Ok(())
    }
    fn validate_snapshot(&self) -> Result<()> {
        let stat = std::fs::metadata(&self.path)?;
        ensure!(
            reset_token(&self.path)? == self.reset
                && Some((stat.dev(), stat.ino())) == self.identity
                && stat.len() >= self.size,
            "log generation changed"
        );
        ensure!(
            stat.len() != self.size || (stat.mtime(), stat.mtime_nsec()) == self.mtime,
            "log changed during read"
        );
        Ok(())
    }
    fn offset(&mut self, ordinal: usize) -> Result<u64> {
        if ordinal == 0 {
            return Ok(0);
        }
        if ordinal > self.complete {
            return Ok(self.size);
        }
        self.index.seek(SeekFrom::Start((ordinal as u64 - 1) * 8))?;
        let mut bytes = [0u8; 8];
        self.index.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
    fn read(&self, start: u64, count: usize) -> Result<Vec<u8>> {
        ensure!(
            reset_token(&self.path)? == self.reset,
            "log generation changed"
        );
        let mut source = File::open(&self.path)?;
        let stat = source.metadata()?;
        ensure!(
            Some((stat.dev(), stat.ino())) == self.identity && stat.len() >= self.size,
            "log generation changed"
        );
        ensure!(
            stat.len() != self.size || (stat.mtime(), stat.mtime_nsec()) == self.mtime,
            "log changed during read"
        );
        source.seek(SeekFrom::Start(start))?;
        let mut data = vec![0u8; count];
        source.read_exact(&mut data)?;
        self.validate_snapshot()?;
        Ok(data)
    }
    pub fn raw(&mut self, reference: &RawReference, offset: u64, limit: usize) -> Result<Vec<u8>> {
        ensure!((1..=SCAN_BYTES).contains(&limit), "invalid raw read bounds");
        self.refresh()?;
        ensure!(reference.generation == self.generation, "stale generation");
        ensure!(
            reference.byte_start <= reference.byte_end && reference.byte_end <= self.size,
            "invalid raw reference"
        );
        let start = reference
            .byte_start
            .saturating_add(offset)
            .min(reference.byte_end);
        self.read(
            start,
            (reference.byte_end - start).min(limit as u64) as usize,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn window(
        &mut self,
        action: WindowAction,
        current: usize,
        count: usize,
        shift: usize,
        codec: Codec,
        max_bytes: usize,
        generation: Option<&str>,
    ) -> Result<LogWindow> {
        ensure!(
            (1..=1000).contains(&count) && (512..=RESPONSE_BYTES).contains(&max_bytes),
            "invalid window budget"
        );
        ensure!(
            matches!(codec, Codec::Messagepack) == matches!(self.codec, Codec::Messagepack),
            "codec does not match index framing"
        );
        self.refresh()?;
        if let Some(generation) = generation {
            ensure!(generation == self.generation, "stale generation");
        }
        let pending_bytes = self.size - self.last_end;
        let total = self.complete
            + usize::from(pending_bytes > 0 && !matches!(self.codec, Codec::Messagepack));
        ensure!(shift > 0, "invalid window navigation");
        let tail = matches!(action, WindowAction::Tail | WindowAction::Older);
        let boundary = if matches!(action, WindowAction::Tail) {
            total
        } else {
            current.min(total)
        };
        let length = if matches!(action, WindowAction::Older | WindowAction::Newer) {
            count.min(shift)
        } else {
            count
        };
        let start = if tail {
            boundary.saturating_sub(length)
        } else {
            boundary
        };
        let stop = if tail {
            boundary
        } else {
            total.min(start.saturating_add(length))
        };
        let mut result = LogWindow {
            pending_bytes,
            generation: self.generation.clone(),
            start: boundary,
            end: boundary,
            total,
            at_start: boundary == 0,
            at_tail: boundary == total,
            records: vec![],
        };
        let indices: Vec<usize> = if tail {
            (start..stop).rev().collect()
        } else {
            (start..stop).collect()
        };
        for ordinal in indices {
            let byte_start = self.offset(ordinal)?;
            let byte_end = self.offset(ordinal + 1)?;
            let raw = RawReference {
                generation: self.generation.clone(),
                byte_start,
                byte_end,
            };
            let record = if byte_end - byte_start > PARSE_BYTES as u64 {
                let data = self.read(byte_start, RECORD_BYTES + 4)?;
                let text = String::from_utf8_lossy(&data);
                let mut end = text.len().min(RECORD_BYTES);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                RecordProjection {
                    text: text[..end].to_owned(),
                    raw,
                    omissions: vec![],
                    diagnostic: Some("parse_budget_exceeded".into()),
                }
            } else {
                project_record(
                    &self.read(byte_start, (byte_end - byte_start) as usize)?,
                    raw,
                    codec,
                    RECORD_BYTES,
                    PARSE_BYTES,
                )?
            };
            let previous = (result.start, result.end, result.at_start, result.at_tail);
            if tail {
                result.records.insert(0, record);
            } else {
                result.records.push(record);
            }
            result.start = if tail { ordinal } else { start };
            result.end = if tail { stop } else { ordinal + 1 };
            result.at_start = result.start == 0;
            result.at_tail = result.end == total;
            if serde_json::to_vec(&result)?.len() > max_bytes {
                if tail {
                    result.records.remove(0);
                } else {
                    result.records.pop();
                }
                (result.start, result.end, result.at_start, result.at_tail) = previous;
                if result.records.is_empty() {
                    bail!("window budget cannot fit first record; increase max_bytes");
                }
                break;
            }
        }
        self.validate_snapshot()?;
        Ok(result)
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn snapshot_rejects_replacement_and_reset_but_allows_append() {
        for mutation in ["replace", "reset", "append"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("stdout");
            std::fs::write(&path, b"old\n").unwrap();
            let mut index = LineIndex::new(path.clone()).unwrap();
            index.refresh().unwrap();
            assert_eq!(index.read(0, 4).unwrap(), b"old\n");
            match mutation {
                "replace" => {
                    let replacement = path.with_extension("new");
                    std::fs::write(&replacement, b"new\n").unwrap();
                    std::fs::rename(replacement, &path).unwrap();
                }
                "reset" => {
                    mark_log_reset(&path).unwrap();
                }
                _ => {
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap()
                        .write_all(b"new\n")
                        .unwrap();
                }
            }
            assert_eq!(index.validate_snapshot().is_ok(), mutation == "append");
            index.refresh().unwrap();
            assert!(index.validate_snapshot().is_ok());
        }
    }
}
