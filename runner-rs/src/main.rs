//! DSH execution plane: hub, executor and one detached shim per execution.
mod capture;
mod dashboard;
mod exec;
mod hub;
mod inspect;
mod legacy;
mod lock;
mod protocol;
mod registry;
mod server;
mod shim;
mod stdio;
mod store;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
        if let Some(value) = arg.strip_prefix(&format!("{name}=")) {
            return Some(value.into());
        }
    }
    None
}

fn arg_present(name: &str) -> bool {
    std::env::args()
        .skip(1)
        .any(|arg| arg == name || arg.starts_with(&format!("{name}=")))
}

fn arg_number(name: &str, default: u64) -> u64 {
    arg_value(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct Paths {
    state: PathBuf,
    spill: PathBuf,
    history: PathBuf,
    socket: String,
    executor_socket: String,
    listen: String,
}
impl Paths {
    fn resolve() -> Self {
        let home = std::env::var_os("DSH_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| ".".into())).join(".dsh")
            });
        let base = home.join("shellout");
        let state = arg_value("--state")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("DSH_SHELLOUT_STATE").map(PathBuf::from))
            .unwrap_or_else(|| base.join("state"));
        let spill = base.join("spill");
        let history = arg_value("--history")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("DSH_SHELLOUT_HISTORY").map(PathBuf::from))
            .unwrap_or_else(|| base.join("history.sqlite"));
        let socket = arg_value("--socket")
            .or_else(|| std::env::var("DSH_SHELLOUT_SOCKET").ok())
            .unwrap_or_else(|| base.join("control.sock").display().to_string());
        let executor_socket = arg_value("--executor-socket")
            .or_else(|| std::env::var("DSH_SHELLOUT_EXECUTOR_SOCKET").ok())
            .unwrap_or_else(|| socket.clone());
        let listen = arg_value("--listen")
            .or_else(|| std::env::var("DSH_SHELLOUT_LISTEN").ok())
            .unwrap_or_else(|| "127.0.0.1:3001".into());
        Self {
            state,
            spill,
            history,
            socket,
            executor_socket,
            listen,
        }
    }
}

fn main() -> std::io::Result<()> {
    // The executor serves no HTTP face; a deployment still asking for one must
    // not start without a dashboard.
    if arg_present("--dashboard") || std::env::var_os("DSH_SHELLOUT_DASHBOARD").is_some() {
        return Err(std::io::Error::other(
            "--dashboard is removed: the executor has no HTTP listener; start the hub with --mode hub --listen <address>",
        ));
    }
    let paths = Paths::resolve();
    let mode = arg_value("--mode").unwrap_or_else(|| "executor".into());
    if mode == "status" {
        // Operator view: which executor owns this state directory and what it
        // is running, read from files rather than from process scanning.
        std::process::exit(inspect::status(&paths.state, arg_present("--json")));
    }
    if mode == "shim" {
        let id = arg_value("--id").ok_or_else(|| std::io::Error::other("shim requires --id"))?;
        return shim::run(&paths.state, &id);
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            if mode == "hub" {
                let store = hub::open_store(&paths.history);
                let hub = Arc::new(hub::Hub::new(store, paths.executor_socket));
                return tokio::select! {
                    result = hub::serve(paths.listen,hub) => result,
                    _ = shutdown_signal() => Ok(()),
                };
            }
            let stdio_mode = mode == "stdio";
            if mode != "executor" && !stdio_mode {
                return Err(std::io::Error::other(format!("unknown mode {mode}")));
            }
            // A session that finds an owner already serving this state root
            // becomes a bridge instead of a second supervisor.
            if stdio_mode {
                // The owner listens where `--socket` points, which is not
                // necessarily inside the state directory.
                if stdio::bridge(std::path::Path::new(&paths.socket)).await? {
                    return Ok(());
                }
            }
            // One executor owns a state directory. Take the claim before the
            // socket is unlinked, so a refused start leaves the live one intact.
            let _claim = lock::StateLock::acquire(&paths.state)?;
            let cwd = arg_value("--cwd").unwrap_or(std::env::current_dir()?.display().to_string());
            let config = server::Config {
                socket_path: paths.socket.clone(),
                cwd,
                lease_ttl_ms: arg_number("--lease-ttl-ms", 60_000),
                max_concurrent: arg_number("--max-concurrent", 64) as usize,
                max_frame_bytes: arg_number(
                    "--max-frame-bytes",
                    protocol::DEFAULT_MAX_FRAME_BYTES as u64,
                ) as usize,
                timeout_ms: arg_number("--timeout-ms", 120_000),
                max_timeout_ms: arg_number("--max-timeout-ms", 600_000),
                max_output_bytes: arg_number("--max-output-bytes", 64_000) as usize,
                spill_dir: paths.spill.display().to_string(),
            };
            let store = match store::Store::open(&paths.history) {
                Ok(store) => Some(Arc::new(store)),
                Err(error) => {
                    eprintln!("history {}: {error}", paths.history.display());
                    None
                }
            };
            let registry = registry::Registry::with_spill_policy(
                config.max_concurrent,
                Duration::from_millis(config.lease_ttl_ms),
                store,
                paths.state.clone(),
                Duration::from_millis(arg_number(
                    "--state-retention-ms",
                    registry::DEFAULT_STATE_RETENTION.as_millis() as u64,
                )),
                Duration::from_millis(arg_number(
                    "--spill-retention-ms",
                    registry::DEFAULT_SPILL_RETENTION.as_millis() as u64,
                )),
                arg_number("--spill-max-bytes", registry::DEFAULT_SPILL_BUDGET_BYTES),
            )?;
            eprintln!(
                "dsh-shellout-runner {} listening on {}",
                env!("CARGO_PKG_VERSION"),
                paths.socket
            );
            // Publish identity so operators and scripts can find this process
            // by what it wrote rather than by a pgrep pattern that also matches
            // the shell running the query.
            inspect::publish(&paths.state, &paths.socket)?;
            let outcome = if stdio_mode {
                // Serve both faces: this ssh connection and the local unix
                // socket, so a second session bridges rather than competing for
                // the state root. When ssh closes, this process leaves the
                // shims and commands behind and exits.
                let unix = server::serve(config.clone(), Arc::clone(&registry));
                tokio::select! {
                    result = stdio::serve(config, Arc::clone(&registry)) => result,
                    result = unix => result,
                    _ = shutdown_signal() => Ok(()),
                }
            } else {
                tokio::select! {
                    result = server::serve(config, registry) => result,
                    // Dropping the listener/runtime leaves detached shims and commands untouched.
                    _ = shutdown_signal() => Ok(()),
                }
            };
            inspect::withdraw(&paths.state);
            outcome
        })
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! { _ = term.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
}
