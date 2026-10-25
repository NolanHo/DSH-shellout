//! DSH-shellout runner: the execution plane.
//!
//! Owns agent-issued process groups and serves the socket the web host
//! connects to. It holds no sessions, no model connection and no web surface.

mod capture;
mod exec;
mod protocol;
mod registry;
mod server;

use std::time::Duration;

use server::Config;

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
    };

    eprintln!("dsh-shellout-runner {} listening on {}", env!("CARGO_PKG_VERSION"), socket_path);

    tokio::select! {
        result = server::serve(config) => result,
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
