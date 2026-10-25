//! Durable execution history.
//!
//! The runner's memory answers "what is happening now"; this store answers
//! "what happened". It keeps one row per execution in SQLite so a restart of
//! the execution plane no longer erases the record, and it is deliberately
//! independent of any live state: the dashboard reads memory for running work
//! and the store for everything older.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

/// One persisted execution.
pub struct Record {
    /// Execution identity.
    pub id: String,
    /// Full argv as JSON.
    pub argv: String,
    /// Working directory.
    pub cwd: String,
    /// Epoch milliseconds.
    pub started_at: u64,
    /// Epoch milliseconds, absent while running.
    pub finished_at: Option<u64>,
    /// `running`, `exited` or `killed`.
    pub state: String,
    /// Exit code.
    pub exit_code: Option<i64>,
    /// Terminating signal.
    pub signal: Option<i64>,
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
    /// Whether capture dropped the head.
    pub lossy: bool,
}

/// The history database.
pub struct Store {
    connection: Mutex<Connection>,
}

impl Store {
    /// Open (or create) the database at `path`.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                argv TEXT NOT NULL,
                cwd TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                state TEXT NOT NULL,
                exit_code INTEGER,
                signal INTEGER,
                stdout TEXT NOT NULL,
                stderr TEXT NOT NULL,
                lossy INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS executions_started ON executions(started_at DESC);",
        )?;
        // A row still marked running belongs to a previous process that died
        // with it; nothing will ever finish it.
        connection.execute(
            "UPDATE executions SET state = 'lost', finished_at = COALESCE(finished_at, ?1) WHERE state = 'running'",
            params![epoch_millis_now()],
        )?;
        Ok(Store { connection: Mutex::new(connection) })
    }

    /// Insert or replace one execution.
    pub fn save(&self, record: &Record) {
        let Ok(connection) = self.connection.lock() else { return };
        let _ = connection.execute(
            "INSERT INTO executions (id, argv, cwd, started_at, finished_at, state, exit_code, signal, stdout, stderr, lossy)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
               finished_at=excluded.finished_at, state=excluded.state,
               exit_code=excluded.exit_code, signal=excluded.signal,
               stdout=excluded.stdout, stderr=excluded.stderr, lossy=excluded.lossy",
            params![
                record.id, record.argv, record.cwd, record.started_at as i64, record.finished_at.map(|at| at as i64),
                record.state, record.exit_code, record.signal, record.stdout, record.stderr, i64::from(record.lossy),
            ],
        );
    }

    /// Newest `limit` rows, for the dashboard after a restart.
    pub fn recent(&self, limit: usize) -> Vec<(String, String, String)> {
        let Ok(connection) = self.connection.lock() else { return Vec::new() };
        let mut statement = match connection.prepare(
            "SELECT id, argv, cwd, started_at, finished_at, state, exit_code, signal, stdout, stderr, lossy
             FROM executions ORDER BY started_at DESC LIMIT ?1",
        ) {
            Ok(statement) => statement,
            Err(_) => return Vec::new(),
        };
        let rows = statement.query_map(params![limit as i64], |row| {
            Ok(json_row(
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?,
            ))
        });
        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Keep only the newest `keep` finished rows.
    pub fn prune(&self, keep: usize) {
        let Ok(connection) = self.connection.lock() else { return };
        let _ = connection.execute(
            "DELETE FROM executions WHERE id NOT IN (
                SELECT id FROM executions ORDER BY started_at DESC LIMIT ?1
            )",
            params![keep as i64],
        );
    }
}

/// Wall clock now, in epoch milliseconds.
fn epoch_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or(0)
}

/// One stored row rendered as the dashboard's JSON document.
fn json_row(
    id: String,
    argv: String,
    cwd: String,
    started_at: i64,
    finished_at: Option<i64>,
    state: String,
    exit_code: Option<i64>,
    signal: Option<i64>,
    stdout: String,
    stderr: String,
    lossy: i64,
) -> (String, String, String) {
    let argv_value: serde_json::Value = serde_json::from_str(&argv).unwrap_or(serde_json::json!([]));
    let document = serde_json::json!({
        "id": id,
        "argv": argv_value,
        "command": argv_value.get(0).and_then(|value| value.as_str()).unwrap_or("bash"),
        "cwd": cwd,
        "state": state,
        "exitCode": exit_code.map(|code| code as i32),
        "signal": signal.map(|value| value as i32),
        "startedAtMs": started_at as u64,
        "finishedAtMs": finished_at.map(|value| value as u64),
        "durationMs": finished_at.map(|end| (end - started_at).max(0) as u64),
        "ageMs": finished_at.map(|_| 0u64).unwrap_or(0),
        "stdout": stdout,
        "stderr": stderr,
        "lossy": lossy != 0,
    });
    (id, String::new(), document.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, started_at: u64, state: &str) -> Record {
        Record {
            id: id.to_string(),
            argv: "[\"bash\",\"-c\",\"echo hi\"]".to_string(),
            cwd: "/tmp".to_string(),
            started_at,
            finished_at: Some(started_at + 5),
            state: state.to_string(),
            exit_code: Some(0),
            signal: None,
            stdout: "hi\n".to_string(),
            stderr: String::new(),
            lossy: false,
        }
    }

    #[test]
    fn saves_and_prunes_history() {
        let dir = std::env::temp_dir().join(format!("dsh-store-test-{}", std::process::id()));
        let path = dir.join("history.sqlite");
        let store = Store::open(&path).expect("open");
        for index in 0..5 {
            store.save(&record(&format!("exec-{index}"), 1_000 + index, "exited"));
        }
        store.prune(2);
        let connection = Connection::open(&path).expect("reopen");
        let count: i64 = connection.query_row("SELECT COUNT(*) FROM executions", [], |row| row.get(0)).expect("count");
        assert_eq!(count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
