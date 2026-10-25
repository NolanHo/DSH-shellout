//! One executor per state directory.
//!
//! Two executors sharing a state directory would each adopt the other's shims,
//! and the older lease would terminate the newer executor's commands. The claim
//! is a non-blocking `flock` on `<state>/.lock`, taken before the socket is
//! unlinked or bound so a refused start leaves the running executor alone.
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// An exclusive claim on a state directory; released when this value drops.
#[derive(Debug)]
pub struct StateLock {
    /// Holding the descriptor holds the lock. Closing it releases the claim.
    _file: File,
}

impl StateLock {
    /// Take the claim without waiting.
    ///
    /// @param state - the state directory this executor would own.
    /// @returns the held claim.
    /// @throws when another live process holds the claim.
    pub fn acquire(state: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(state)?;
        let path = state.join(".lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)?;
        // SAFETY: `file` owns a valid descriptor for the duration of the call.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "state directory {} is already owned by another executor; refusing to start ({error})",
                    state.display()
                ),
            ));
        }
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_executor_cannot_claim_the_same_state() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/lock");
        let first = StateLock::acquire(&dir).expect("first claim");
        let error = StateLock::acquire(&dir).expect_err("second claim must be refused");
        assert!(
            error
                .to_string()
                .contains("already owned by another executor"),
            "{error}"
        );
        drop(first);
        // The released claim is immediately available again.
        StateLock::acquire(&dir).expect("claim after release");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn separate_state_directories_do_not_contend() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/lock-separate");
        let _first = StateLock::acquire(&root.join("a")).expect("first claim");
        StateLock::acquire(&root.join("b")).expect("independent claim");
        std::fs::remove_dir_all(root).unwrap();
    }
}
