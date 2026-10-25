//! Bounded per-stream capture: an in-memory tail plus an optional spill file.
//!
//! Mirrors the harness collect-mode contract the host relies on: in-memory
//! retention keeps the **tail**, a read below the retained window reports
//! `lossy`, and a complete stream file is offered by path while it exists.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;

/// One offset read.
pub struct ReadResult {
    /// Text from the requested offset (the retained tail when lossy).
    pub text: String,
    /// Absolute byte offset to resume from.
    pub next_offset: u64,
    /// True when the requested offset left the retained window.
    pub lossy: bool,
    /// Complete-stream file, when one is still held.
    pub spill_path: Option<String>,
}

/// Retained bytes and spill state for one stream.
pub struct CaptureState {
    tail: Vec<u8>,
    total: u64,
    max_bytes: usize,
    lossy: bool,
    spill: Option<File>,
    spill_path: Option<PathBuf>,
    spill_len: u64,
    spill_max: u64,
}

impl CaptureState {
    /// Create a capture bounded by `max_bytes`, spilling into `spill_dir`.
    pub fn new(max_bytes: usize, spill_max: u64, spill_dir: &str, execution_id: &str, stream: &str) -> Self {
        let path = PathBuf::from(spill_dir).join(format!("dsh-shellout-{execution_id}-{stream}.log"));
        let spill = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .ok();
        let spill_path = if spill.is_some() { Some(path) } else { None };
        CaptureState {
            tail: Vec::new(),
            total: 0,
            max_bytes: max_bytes.max(1),
            lossy: false,
            spill,
            spill_path,
            spill_len: 0,
            spill_max,
        }
    }

    /// Append captured bytes.
    pub fn push(&mut self, chunk: &[u8]) {
        self.total += chunk.len() as u64;
        self.tail.extend_from_slice(chunk);
        if self.tail.len() > self.max_bytes {
            let drop = self.tail.len() - self.max_bytes;
            self.tail.drain(..drop);
            self.lossy = true;
        }
        if let Some(file) = self.spill.as_mut() {
            if self.spill_len + chunk.len() as u64 > self.spill_max {
                self.spill = None;
                self.spill_path = None;
                self.lossy = true;
            } else if let Err(error) = file.write_all(chunk) {
                if error.kind() != ErrorKind::Other {
                    self.spill = None;
                    self.spill_path = None;
                }
            } else {
                self.spill_len += chunk.len() as u64;
            }
        }
    }

    /// Read a byte range, reporting loss when the offset left the window.
    pub fn read_from(&self, offset: u64, limit: usize) -> ReadResult {
        let retained_start = self.total - self.tail.len() as u64;
        let (slice, lossy) = if offset < retained_start {
            (&self.tail[..], true)
        } else {
            let start = (offset - retained_start) as usize;
            let end = (start + limit.max(1)).min(self.tail.len());
            (&self.tail[start..end], self.lossy && self.total > self.tail.len() as u64)
        };
        let usable = utf8_prefix_len(slice);
        ReadResult {
            text: String::from_utf8_lossy(&slice[..usable]).into_owned(),
            next_offset: retained_start + (offset.saturating_sub(retained_start)).saturating_add(usable as u64).min(self.total),
            lossy,
            spill_path: self.spill_path.as_ref().map(|path| path.display().to_string()),
        }
    }

    /// Absolute offset just past the last captured byte.
    pub fn total(&self) -> u64 {
        self.total
    }
}

/// Length of the longest valid UTF-8 prefix of `bytes`.
fn utf8_prefix_len(bytes: &[u8]) -> usize {
    match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(error) => error.valid_up_to(),
    }
}
