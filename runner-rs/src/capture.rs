//! Offset reads from the shim's append-only, byte-capped logs.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::{json, Value};

pub const READ_LIMIT: usize = 256 * 1024;

pub fn read_stream(
    dir: &Path,
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
    let path = dir.join(format!("{stream}.log"));
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(
                json!({"text":"", "nextOffset":offset,"lossy":truncated,"truncated":truncated}),
            )
        }
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
    Ok(json!({"text":text,"nextOffset":offset + bytes.len() as u64,
        "lossy":truncated || invalid,"truncated":truncated,
        "spillPath": if truncated { None } else { Some(path.display().to_string()) }}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_offsets_limits_and_binary_bytes() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/file-offsets");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("stdout.log"), b"hello\xffworld").unwrap();
        let first = read_stream(&dir, "stdout", 0, 5, false).unwrap();
        assert_eq!(first["text"], "hello");
        assert_eq!(first["nextOffset"], 5);
        let second = read_stream(&dir, "stdout", 5, 5, true).unwrap();
        assert_eq!(second["nextOffset"], 10);
        assert_eq!(second["text"], "\u{fffd}worl");
        assert_eq!(second["lossy"], true);
        let beyond = read_stream(&dir, "stdout", 100, 5, false).unwrap();
        assert_eq!(beyond["text"], "");
        assert_eq!(beyond["nextOffset"], 100);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
