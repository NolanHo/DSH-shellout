//! DSH-shellout runner: the execution plane.
//!
//! Owns agent-issued process groups and serves the socket the web host
//! connects to. It holds no sessions, no model connection and no web surface.

mod capture;
mod dashboard;
mod exec;
mod legacy;
mod protocol;
mod registry;
mod server;
mod hub;
mod store;

use std::time::Duration;

use server::Config;

/// The dashboard markup, shared by the in-process and hub faces.
fn dashboard_page() -> &'static str {
    dashboard::page()
}

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
        if let Some(rest) = arg.strip_prefix(&format!("{name}=")) {
            return Some(rest.to_string());
        }
    }
    None
}

fn arg_number(name: &str, default: u64) -> u64 {
    arg_value(name)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let home = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());
    let socket_path = arg_value("--socket")
        .or_else(|| std::env::var("DSH_SHELLOUT_SOCKET").ok())
        .unwrap_or_else(|| format!("{home}/dsh-shellout/control.sock"));
    let cwd = arg_value("--cwd").unwrap_or_else(|| {
        std::env::current_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| "/".to_string())
    });

    let config = Config {
        socket_path: socket_path.clone(),
        cwd,
        lease_ttl_ms: arg_number("--lease-ttl-ms", 60_000),
        max_concurrent: arg_number("--max-concurrent", 64) as usize,
        max_frame_bytes: arg_number("--max-frame-bytes", 8 * 1024 * 1024) as usize,
        timeout_ms: arg_number("--timeout-ms", 120_000),
        max_timeout_ms: arg_number("--max-timeout-ms", 600_000),
        max_output_bytes: arg_number("--max-output-bytes", 64_000) as usize,
        spill_dir: std::env::temp_dir().display().to_string(),
    };
    let dashboard_address = arg_value("--dashboard")
        .or_else(|| std::env::var("DSH_SHELLOUT_DASHBOARD").ok())
        .unwrap_or_default();

    if arg_value("--mode").as_deref() == Some("hub") {
        let history = arg_value("--history").unwrap_or_else(|| "/root/.dsh-runner/shellout/history.sqlite".to_string());
        let store = hub::open_store(std::path::Path::new(&history)).expect("history database");
        let executor_socket = arg_value("--executor-socket").unwrap_or_else(|| "/root/.dsh/shellout/control.sock".to_string());
        let hub = std::sync::Arc::new(hub::Hub { store, executor_socket });
        return hub::serve(socket_path, hub).await;
    }
    eprintln!("dsh-shellout-runner {} listening on {}", env!("CARGO_PKG_VERSION"), socket_path);

    let history_path = arg_value("--history").map(std::path::PathBuf::from).unwrap_or_else(|| {
        let home = std::env::var("DSH_HOME").unwrap_or_else(|_| "/root/.dsh".to_string());
        std::path::PathBuf::from(home).join("shellout").join("history.sqlite")
    });
    let store = match store::Store::open(&history_path) {
        Ok(store) => {
            eprintln!("history: {}", history_path.display());
            Some(std::sync::Arc::new(store))
        }
        Err(error) => {
            eprintln!("history: cannot open {}: {error}", history_path.display());
            None
        }
    };
    let (registry, _events) = registry::Registry::with_store(
        config.max_concurrent,
        Duration::from_millis(config.lease_ttl_ms),
        store,
    );
    if !dashboard_address.is_empty() {
        let registry = std::sync::Arc::clone(&registry);
        tokio::spawn(async move {
            let _ = dashboard::serve(dashboard_address, registry).await;
        });
    }
    tokio::select! {
        result = server::serve(config, registry) => result,
        _ = shutdown_signal() => {
            // A supervisor stop: give live executions a short window to end.
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    tokio::select! {
        _ = term.recv() => {}
        _ = async {
            match interrupt.as_mut() {
                Some(signal) => { signal.recv().await; }
                None => { let _ = tokio::signal::ctrl_c().await; }
            }
        } => {}
    }
}
