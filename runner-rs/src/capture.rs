//! Offset reads from the shim's append-only, byte-capped logs.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::{json, Value};

pub const READ_LIMIT: usize = 256 * 1024;

/// Read one stream from the execution directory, falling back to the host-owned
/// spill when retention has already deleted that directory.
///
/// @param dir - the execution's ephemeral state directory.
/// @param spill - complete capture in the host's spill directory, when it exists.
/// @param stream - `stdout` or `stderr`.
/// @param offset - absolute byte offset to resume from.
/// @param limit - byte budget for this reply.
/// @param truncated - whether the capture itself is only a prefix.
/// @returns the text, the resume offset, and the loss and spill facts.
pub fn read_stream(
    dir: &Path,
    spill: Option<&Path>,
    stream: &str,
    offset: u64,
    limit: usize,
    truncated: bool,
) -> io::Result<Value> {
    if !matches!(stream, "stdout" | "stderr") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "stream must be stdout or stderr",
        ));
    }
    let state_path = dir.join(format!("{stream}.log"));
    let mut file = match File::open(&state_path) {
        Ok(file) => file,
        // The state directory is a working copy; the spill outlives it.
        Err(error) if error.kind() == io::ErrorKind::NotFound => match spill {
            Some(path) => File::open(path)?,
            None => {
                // A directory that still exists has simply not written yet; one
                // that is gone lost the bytes a reader was promised.
                let lossy = truncated || !dir.exists();
                return Ok(json!({"text":"", "nextOffset":offset,
                    "lossy":lossy,"truncated":truncated}));
            }
        },
        Err(error) => return Err(error),
    };
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; limit.min(READ_LIMIT)];
    let count = file.read(&mut bytes)?;
    bytes.truncate(count);
    // Keep a partial trailing UTF-8 sequence for the next read. Invalid bytes
    // are replaced, but always consume input so binary output cannot stall.
    if let Err(error) = std::str::from_utf8(&bytes) {
        if error.error_len().is_none() && error.valid_up_to() > 0 {
            bytes.truncate(error.valid_up_to());
        }
    }
    let text = String::from_utf8_lossy(&bytes);
    let invalid = matches!(&text, std::borrow::Cow::Owned(_));
    // `spillPath` is omitted rather than null: the wire contract makes it an
    // optional string, and a null reaches strict consumers as a type violation.
    let mut reply = json!({"text":text,"nextOffset":offset + bytes.len() as u64,
    "lossy":truncated || invalid,"truncated":truncated});
    if !truncated {
        if let Some(path) = spill {
            reply["spillPath"] = serde_json::Value::String(path.display().to_string());
        }
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_spill_is_omitted_rather_than_null() {
        // The wire contract makes `spillPath` an optional string. A null value
        // reaches a strict consumer as a type violation, so it must be absent.
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/no-spill");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stdout.log"), b"ok\n").unwrap();
        let reply = read_stream(&dir, None, "stdout", 0, 64, false).unwrap();
        assert_eq!(reply["text"], "ok\n");
        assert!(
            reply.get("spillPath").is_none(),
            "spillPath must be absent, got {:?}",
            reply.get("spillPath")
        );
    }

    #[test]
    fn file_offsets_limits_and_binary_bytes() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/file-offsets");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stdout.log"), b"hello\xffworld").unwrap();
        let first = read_stream(&dir, None, "stdout", 0, 5, false).unwrap();
        assert_eq!(first["text"], "hello");
        assert_eq!(first["nextOffset"], 5);
        let second = read_stream(&dir, None, "stdout", 5, 5, true).unwrap();
        assert_eq!(second["nextOffset"], 10);
        assert_eq!(second["text"], "\u{fffd}worl");
        assert_eq!(second["lossy"], true);
        let beyond = read_stream(&dir, None, "stdout", 100, 5, false).unwrap();
        assert_eq!(beyond["text"], "");
        assert_eq!(beyond["nextOffset"], 100);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_deleted_capture_is_lossy_but_an_unwritten_one_is_empty() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/missing-capture");
        let _ = std::fs::remove_dir_all(&root);
        // The execution directory is gone: its capture went with it.
        let gone = root.join("gone");
        let read = read_stream(&gone, None, "stdout", 0, 8, false).unwrap();
        assert_eq!(read["text"], "");
        assert_eq!(read["lossy"], true);
        // A directory that exists but has not written yet is merely empty.
        std::fs::create_dir_all(root.join("starting")).unwrap();
        let read = read_stream(&root.join("starting"), None, "stdout", 0, 8, false).unwrap();
        assert_eq!(read["lossy"], false);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_spill_serves_the_capture_after_the_state_directory_is_gone() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/spill-fallback");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let spill = root.join("spill-stdout.log");
        std::fs::write(&spill, b"kept\n").unwrap();
        let read = read_stream(&root.join("gone"), Some(&spill), "stdout", 0, 8, false).unwrap();
        assert_eq!(read["text"], "kept\n");
        assert_eq!(read["lossy"], false);
        assert_eq!(read["spillPath"], spill.display().to_string());
        std::fs::remove_dir_all(root).unwrap();
    }
}
