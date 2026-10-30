//! Remote attach over stdio.
//!
//! `ssh host -- dsh-shellout-runner --mode stdio` turns an ssh connection into
//! the same wire the unix socket speaks. ssh supplies identity, `ProxyJump` and
//! keepalives, so the runner opens no port and adds no credential of its own.
//!
//! One process per state root supervises. When an executor already owns the
//! host's socket this process is only a bridge: bytes in, bytes out, with no
//! second registry and no competing history writer.

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::registry::Registry;
use crate::server::{handle_stream, Config};

/// Attach the local standard streams to an executor's socket.
///
/// @param socket - the state root's control socket.
/// @returns `true` when an executor answered and the bridge ran to completion,
///   `false` when no executor owns this state root (the caller becomes one).
pub async fn bridge(socket: &Path) -> std::io::Result<bool> {
    let stream = match UnixStream::connect(socket).await {
        Ok(stream) => stream,
        Err(_) => return Ok(false),
    };
    let (mut socket_reader, mut socket_writer) = stream.into_split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    // Frame-preserving copy in both directions: the socket side speaks the same
    // protocol, so re-encoding would buy nothing and risk a split frame.
    let upstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = stdin.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            socket_writer.write_all(&buffer[..read]).await?;
            socket_writer.flush().await?;
        }
        socket_writer.shutdown().await
    };
    let downstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = socket_reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            stdout.write_all(&buffer[..read]).await?;
            stdout.flush().await?;
        }
        stdout.flush().await
    };
    let _ = tokio::try_join!(upstream, downstream);
    Ok(true)
}

/// Serve this process's registry over stdio.
pub async fn serve(config: Config, registry: Arc<Registry>) -> std::io::Result<()> {
    let reader = tokio::io::stdin();
    let writer = tokio::io::stdout();
    handle_stream(reader, writer, registry, config).await
}
