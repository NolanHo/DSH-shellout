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
            // The requested offset is still retained, so this read is complete
            // even when earlier bytes were dropped.
            (&self.tail[start..end], false)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn capture(max_bytes: usize, spill_max: u64) -> CaptureState {
        CaptureState::new(max_bytes, spill_max, "/tmp", "test-execution", "stdout")
    }

    #[test]
    fn keeps_the_tail_and_reports_loss_below_the_window() {
        let mut state = capture(4, 1_048_576);
        state.push(b"abcdef");
        assert_eq!(state.total(), 6);
        let lost = state.read_from(0, 16);
        assert!(lost.lossy);
        assert_eq!(lost.text, "cdef");
        assert_eq!(lost.next_offset, 6);
    }

    #[test]
    fn reads_within_the_window_are_complete() {
        let mut state = capture(4, 1_048_576);
        state.push(b"abcdef");
        let read = state.read_from(4, 16);
        assert!(!read.lossy);
        assert_eq!(read.text, "ef");
        assert_eq!(read.next_offset, 6);
    }

    #[test]
    fn reads_advance_by_the_returned_bytes() {
        let mut state = capture(64, 1_048_576);
        state.push(b"hello world");
        let first = state.read_from(0, 5);
        assert_eq!(first.text, "hello");
        assert_eq!(first.next_offset, 5);
        let second = state.read_from(5, 64);
        assert_eq!(second.text, " world");
        assert_eq!(second.next_offset, 11);
    }

    #[test]
    fn an_oversized_stream_drops_its_spill() {
        let mut state = capture(64, 4);
        state.push(b"123456789");
        assert!(state.read_from(0, 64).spill_path.is_none());
        let read = state.read_from(0, 64);
        assert_eq!(read.text, "123456789");
    }

    #[test]
    fn utf8_boundaries_are_respected() {
        let mut state = capture(64, 1_048_576);
        state.push("héllo".as_bytes());
        let read = state.read_from(0, 2);
        assert_eq!(read.text, "h");
        assert_eq!(read.next_offset, 1);
    }
}
