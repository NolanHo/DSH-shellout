//! Durable execution history. Only the executor opens a writable store; the
//! hub reads summaries and fetches captured output only through detail lookups.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};
use serde_json::{json, Value};

const MAX_RECENT_ROWS: usize = 50;
const SUMMARY_BYTES: usize = 5 * 1024;
const RECENT_BYTES: usize = 256 * 1024;

/// The state-relevant facts of one persisted row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
    pub state: String,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub finished_at: Option<i64>,
}

/// One persisted execution.
pub struct Record {
    pub id: String,
    /// Full argv encoded as a JSON array of strings.
    pub argv: String,
    pub cwd: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub state: String,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub stdout: String,
    pub stderr: String,
    /// Aggregate retained for compatibility with existing history.
    pub lossy: bool,
    pub stdout_lossy: bool,
    pub stderr_lossy: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

struct WriteHealth {
    writable: bool,
    error: Option<String>,
}

pub struct Store {
    connection: Mutex<Connection>,
    write_health: Mutex<WriteHealth>,
    per_stream_flags: bool,
}

impl Store {
    /// Open without migrations or reconciliation. The hub never writes history.
    pub fn open_read_only(path: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        let per_stream_flags = stream_columns(&connection)?.len() == 4;
        Ok(Self {
            connection: Mutex::new(connection),
            write_health: Mutex::new(WriteHealth {
                writable: false,
                error: None,
            }),
            per_stream_flags,
        })
    }

    /// Open for the executor, preserving all existing rows and their states.
    /// Adoption, not database startup, decides whether a running row is lost.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(
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
        let existing = stream_columns(&transaction)?;
        for column in [
            "stdout_lossy",
            "stderr_lossy",
            "stdout_truncated",
            "stderr_truncated",
        ] {
            if !existing.iter().any(|name| name == column) {
                transaction.execute_batch(&format!(
                    "ALTER TABLE executions ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                ))?;
            }
        }
        transaction.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
            write_health: Mutex::new(WriteHealth {
                writable: true,
                error: None,
            }),
            per_stream_flags: true,
        })
    }

    /// Insert one execution, or advance a `running` row to its terminal facts.
    ///
    /// The state machine is one-way: a row is born `running` and only the
    /// transition out of `running` is ever written. A terminal row is final, so
    /// a late or repeated save can never regress it.
    pub fn save(&self, record: &Record) -> Result<(), String> {
        let result = (|| {
            let connection = self.connection.lock().map_err(|error| error.to_string())?;
            connection.execute(
                "INSERT INTO executions
                 (id, argv, cwd, started_at, finished_at, state, exit_code, signal,
                  stdout, stderr, lossy, stdout_lossy, stderr_lossy, stdout_truncated, stderr_truncated)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                 ON CONFLICT(id) DO UPDATE SET
                   argv=excluded.argv, cwd=excluded.cwd, started_at=excluded.started_at,
                   finished_at=excluded.finished_at, state=excluded.state,
                   exit_code=excluded.exit_code, signal=excluded.signal,
                   stdout=excluded.stdout, stderr=excluded.stderr, lossy=excluded.lossy,
                   stdout_lossy=excluded.stdout_lossy, stderr_lossy=excluded.stderr_lossy,
                   stdout_truncated=excluded.stdout_truncated, stderr_truncated=excluded.stderr_truncated
                 WHERE executions.state = 'running'",
                params![
                    record.id, record.argv, record.cwd, record.started_at,
                    record.finished_at, record.state, record.exit_code, record.signal,
                    record.stdout, record.stderr, record.lossy, record.stdout_lossy,
                    record.stderr_lossy, record.stdout_truncated, record.stderr_truncated,
                ],
            ).map_err(|error| error.to_string())?;
            Ok(())
        })();
        let mut health = self
            .write_health
            .lock()
            .map_err(|error| error.to_string())?;
        health.error = result.as_ref().err().cloned();
        result
    }

    /// State-relevant facts of one row, without loading its output.
    pub fn facts(&self, id: &str) -> Result<Option<Facts>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare("SELECT state, exit_code, signal, finished_at FROM executions WHERE id = ?1")
            .map_err(|error| error.to_string())?;
        let mut rows = statement
            .query(params![id])
            .map_err(|error| error.to_string())?;
        let Some(row) = rows.next().map_err(|error| error.to_string())? else {
            return Ok(None);
        };
        Ok(Some(Facts {
            state: row.get(0).map_err(|error| error.to_string())?,
            exit_code: row.get(1).map_err(|error| error.to_string())?,
            signal: row.get(2).map_err(|error| error.to_string())?,
            finished_at: row.get(3).map_err(|error| error.to_string())?,
        }))
    }

    /// Every row still recorded as `running`; startup reconciles them against
    /// the state directory.
    pub fn running_ids(&self) -> Result<Vec<String>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare("SELECT id FROM executions WHERE state = 'running'")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        let mut ids = Vec::new();
        for id in rows {
            ids.push(id.map_err(|error| error.to_string())?);
        }
        Ok(ids)
    }

    /// Advance a `running` row to `lost`. A terminal row is never touched.
    pub fn mark_lost(&self, id: &str, at_ms: u64) -> Result<(), String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        connection
            .execute(
                "UPDATE executions SET state = 'lost', finished_at = ?2
                 WHERE id = ?1 AND state = 'running'",
                params![id, at_ms as i64],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Writer availability and the last write failure (cleared by a successful save).
    /// A read-only hub store intentionally reports historyWritable=false.
    pub fn health(&self) -> Value {
        match self.write_health.lock() {
            Ok(health) => json!({
                "historyWritable": health.writable && health.error.is_none(),
                "historyError": health.error,
            }),
            Err(error) => json!({"historyWritable": false, "historyError": error.to_string()}),
        }
    }

    pub fn count(&self) -> Result<i64, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        connection
            .query_row("SELECT COUNT(*) FROM executions", [], |row| row.get(0))
            .map_err(|error| error.to_string())
    }

    /// At most 50 summaries and 256 KiB serialized as a JSON array. Each row
    /// has a 5 KiB budget; oversize metadata becomes an explicit error row,
    /// never a dropped execution. Huge IDs use a 128-character prefix with
    /// idTruncated=true; their full identity remains available in the database.
    /// SQL bounds metadata before loading it and never selects output blobs.
    pub fn recent(&self, limit: usize) -> Result<Vec<Value>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let query = format!(
            "SELECT
                CASE WHEN length(CAST(id AS BLOB)) <= 512 THEN id ELSE substr(id,1,128) END,
                CASE WHEN length(CAST(argv AS BLOB)) <= 65536 THEN argv ELSE NULL END,
                CASE WHEN length(CAST(cwd AS BLOB)) <= 8192 THEN cwd ELSE NULL END,
                started_at, finished_at, substr(state,1,64), exit_code, signal, lossy,
                {}, length(CAST(id AS BLOB)) > 512,
                length(CAST(state AS BLOB)) > 64
             FROM executions ORDER BY started_at DESC, id DESC LIMIT ?1",
            self.flag_projection(),
        );
        let mut statement = connection
            .prepare(&query)
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![limit.min(MAX_RECENT_ROWS) as i64], |row| {
                let id: String = row.get(0)?;
                let argv: Option<String> = row.get(1)?;
                let cwd: Option<String> = row.get(2)?;
                let state: String = row.get(5)?;
                let id_truncated: bool = row.get(13)?;
                let state_truncated: bool = row.get(14)?;
                let error_row = |error: &str| {
                    json!({
                        "id": id, "state": state, "error": error,
                        "idTruncated": id_truncated,
                    })
                };
                if id_truncated || state_truncated || argv.is_none() || cwd.is_none() {
                    return Ok(error_row("history summary metadata exceeds size limit"));
                }
                let argv = match parse_argv(argv.as_deref().unwrap_or_default()) {
                    Ok(argv) => argv,
                    Err(error) => return Ok(error_row(&error)),
                };
                let summary = document(row, argv, cwd.unwrap_or_default())?;
                if summary.to_string().len() > SUMMARY_BYTES {
                    Ok(error_row("history summary exceeds 5 KiB size limit"))
                } else {
                    Ok(summary)
                }
            })
            .map_err(|error| error.to_string())?;
        // Fifty 5 KiB rows plus commas/brackets fit under the total budget.
        let mut summaries = Vec::new();
        let mut bytes = 2;
        for row in rows {
            let row = row.map_err(|error| error.to_string())?;
            bytes += row.to_string().len() + usize::from(!summaries.is_empty());
            if bytes > RECENT_BYTES {
                return Err("history summaries exceeded byte budget".to_string());
            }
            summaries.push(row);
        }
        Ok(summaries)
    }

    /// Primary-key detail lookup: the only history query that loads output.
    pub fn detail(&self, id: &str) -> Result<Option<Value>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let query = format!(
            "SELECT id, argv, cwd, started_at, finished_at, state, exit_code, signal,
             lossy, {}, stdout, stderr FROM executions WHERE id = ?1",
            self.flag_projection(),
        );
        let mut statement = connection
            .prepare(&query)
            .map_err(|error| error.to_string())?;
        let mut rows = statement
            .query(params![id])
            .map_err(|error| error.to_string())?;
        let Some(row) = rows.next().map_err(|error| error.to_string())? else {
            return Ok(None);
        };
        let argv: String = row.get(1).map_err(|error| error.to_string())?;
        let argv = match parse_argv(&argv) {
            Ok(argv) => argv,
            Err(error) => {
                return Ok(Some(json!({
                    "id": row.get::<_, String>(0).map_err(|error| error.to_string())?,
                    "state": row.get::<_, String>(5).map_err(|error| error.to_string())?,
                    "error": error,
                })))
            }
        };
        let cwd = row.get(2).map_err(|error| error.to_string())?;
        let mut value = document(row, argv, cwd).map_err(|error| error.to_string())?;
        value["stdout"] = Value::String(row.get(13).map_err(|error| error.to_string())?);
        value["stderr"] = Value::String(row.get(14).map_err(|error| error.to_string())?);
        Ok(Some(value))
    }

    fn flag_projection(&self) -> &'static str {
        if self.per_stream_flags {
            "stdout_lossy, stderr_lossy, stdout_truncated, stderr_truncated"
        } else {
            // Legacy rows retain aggregate lossiness; individual facts were
            // not recorded, matching the migration's default values.
            "0, 0, 0, 0"
        }
    }
}

fn stream_columns(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("PRAGMA table_info(executions)")?;
    let names = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for name in names {
        let name = name?;
        if [
            "stdout_lossy",
            "stderr_lossy",
            "stdout_truncated",
            "stderr_truncated",
        ]
        .contains(&name.as_str())
        {
            columns.push(name);
        }
    }
    Ok(columns)
}

fn parse_argv(argv: &str) -> Result<Vec<String>, String> {
    serde_json::from_str::<Vec<String>>(argv)
        .map_err(|_| "malformed history argv: expected JSON array of strings".to_string())
}

/// Shared column layout for summaries and details, without output fields.
fn document(row: &rusqlite::Row<'_>, argv: Vec<String>, cwd: String) -> rusqlite::Result<Value> {
    let started_at: i64 = row.get(3)?;
    let finished_at: Option<i64> = row.get(4)?;
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "command": argv.first(),
        "argv": argv,
        "cwd": cwd,
        "state": row.get::<_, String>(5)?,
        "exitCode": row.get::<_, Option<i64>>(6)?,
        "signal": row.get::<_, Option<i64>>(7)?,
        "startedAtMs": started_at,
        "finishedAtMs": finished_at,
        "durationMs": finished_at.map(|end| end.saturating_sub(started_at).max(0)),
        "ageMs": 0,
        "lossy": row.get::<_, bool>(8)?,
        "stdoutLossy": row.get::<_, bool>(9)?,
        "stderrLossy": row.get::<_, bool>(10)?,
        "stdoutTruncated": row.get::<_, bool>(11)?,
        "stderrTruncated": row.get::<_, bool>(12)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct DatabasePath(std::path::PathBuf);

    impl DatabasePath {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join(".task-tmp")
                .join(format!(
                    "store-test-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
            std::fs::create_dir_all(&dir).expect("create fixture directory");
            Self(dir.join("history.sqlite"))
        }
    }

    impl Drop for DatabasePath {
        fn drop(&mut self) {
            std::fs::remove_dir_all(self.0.parent().unwrap()).expect("remove fixture directory");
        }
    }

    fn record(id: &str) -> Record {
        Record {
            id: id.to_string(),
            argv: "[\"bash\",\"-c\",\"echo hi\"]".to_string(),
            cwd: "/tmp".to_string(),
            started_at: 1000,
            finished_at: Some(1005),
            state: "exited".to_string(),
            exit_code: Some(0),
            signal: None,
            stdout: "hi\n".to_string(),
            stderr: "warning\n".to_string(),
            lossy: false,
            stdout_lossy: false,
            stderr_lossy: false,
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    #[test]
    fn read_only_save_failure_is_reported() {
        let path = DatabasePath::new();
        let writer = Store::open(&path.0).expect("open writer");
        writer.save(&record("existing")).expect("initial save");
        let reader = Store::open_read_only(&path.0).expect("open reader");
        assert!(reader.save(&record("rejected")).is_err());
        assert_eq!(reader.health()["historyWritable"], false);
        assert!(reader.health()["historyError"].is_string());
        assert_eq!(writer.count().unwrap(), 1);
    }

    #[test]
    fn stream_flags_persist_and_output_is_detail_only() {
        let path = DatabasePath::new();
        let store = Store::open(&path.0).unwrap();
        let mut execution = record("flags");
        execution.lossy = true;
        execution.stdout_lossy = true;
        execution.stderr_truncated = true;
        store.save(&execution).unwrap();
        let detail = store.detail("flags").unwrap().unwrap();
        assert_eq!(detail["stdoutLossy"], true);
        assert_eq!(detail["stderrLossy"], false);
        assert_eq!(detail["stdoutTruncated"], false);
        assert_eq!(detail["stderrTruncated"], true);
        assert_eq!(detail["lossy"], true);
        assert_eq!(detail["stdout"], "hi\n");
        assert_eq!(detail["stderr"], "warning\n");
        assert!(store.detail("missing").unwrap().is_none());
        let recent = store.recent(50).unwrap();
        assert!(recent[0].get("stdout").is_none());
        assert!(recent[0].get("stderr").is_none());
        assert_eq!(recent[0]["stderrTruncated"], true);
    }

    #[test]
    fn malformed_argv_is_counted_and_explicit() {
        let path = DatabasePath::new();
        let store = Store::open(&path.0).unwrap();
        for (index, argv) in ["not json", "{}", "[1]", "[\"ok\",null]"]
            .iter()
            .enumerate()
        {
            let mut execution = record(&format!("bad-{index}"));
            execution.argv = argv.to_string();
            store.save(&execution).unwrap();
        }
        assert_eq!(store.count().unwrap(), 4);
        let recent = store.recent(50).unwrap();
        assert_eq!(recent.len(), 4);
        for row in recent {
            assert!(row["error"].as_str().unwrap().contains("argv"));
            assert!(row.get("argv").is_none());
            assert!(
                store.detail(row["id"].as_str().unwrap()).unwrap().unwrap()["error"].is_string()
            );
        }
    }

    #[test]
    fn summaries_bound_large_metadata_without_dropping_rows() {
        let path = DatabasePath::new();
        let store = Store::open(&path.0).unwrap();
        for index in 0..55 {
            let mut execution = record(&format!("exec-{index}"));
            execution.argv = serde_json::to_string(&vec!["x".repeat(70000)]).unwrap();
            execution.cwd = "y".repeat(10000);
            execution.stdout = "z".repeat(100000);
            store.save(&execution).unwrap();
        }
        let mut huge_id = record(&"界".repeat(2000));
        huge_id.started_at = 2000;
        store.save(&huge_id).unwrap();
        let summaries = store.recent(usize::MAX).unwrap();
        assert_eq!(summaries.len(), 50);
        assert!(serde_json::to_vec(&summaries).unwrap().len() <= RECENT_BYTES);
        assert_eq!(summaries[0]["idTruncated"], true);
        assert!(summaries.iter().all(|row| row["error"].is_string()));
        assert!(summaries.iter().all(|row| row.get("stdout").is_none()));
        assert_eq!(store.count().unwrap(), 56);
    }

    #[test]
    fn legacy_migration_preserves_running_rows_and_output() {
        let path = DatabasePath::new();
        let legacy = Connection::open(&path.0).unwrap();
        legacy.execute_batch(
            "CREATE TABLE executions (
                id TEXT PRIMARY KEY, argv TEXT NOT NULL, cwd TEXT NOT NULL,
                started_at INTEGER NOT NULL, finished_at INTEGER, state TEXT NOT NULL,
                exit_code INTEGER, signal INTEGER, stdout TEXT NOT NULL,
                stderr TEXT NOT NULL, lossy INTEGER NOT NULL
             );
             INSERT INTO executions VALUES
                ('legacy', '[\"echo\"]', '/', 1000, NULL, 'running', NULL, NULL, 'old output', '', 1);"
        ).unwrap();
        drop(legacy);
        let reader = Store::open_read_only(&path.0).unwrap();
        assert_eq!(reader.recent(50).unwrap()[0]["state"], "running");
        drop(reader);
        let store = Store::open(&path.0).unwrap();
        let detail = store.detail("legacy").unwrap().unwrap();
        assert_eq!(detail["state"], "running");
        assert_eq!(detail["finishedAtMs"], Value::Null);
        assert_eq!(detail["stdout"], "old output");
        assert_eq!(detail["lossy"], true);
        assert_eq!(detail["stdoutLossy"], false);
        assert_eq!(store.count().unwrap(), 1);
        store.save(&record("new")).unwrap();
        assert_eq!(store.count().unwrap(), 2);
    }
}
