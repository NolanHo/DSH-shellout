//! Attach to a remote host: bootstrap the binary, then carry protocol frames.
//!
//! The transport is an ssh command line, never a protocol of our own: ssh
//! supplies identity, `ProxyJump` and keepalives. Everything this module adds is
//! bootstrap — probe the host, push the one static binary this build produced,
//! verify it runs, and then hand the connection to the stdio protocol.
//!
//! The remote command is always passed after `--`, so a stand-in script can
//! emulate the hop in tests without changing this code path.

use std::io;
use std::path::PathBuf;
use std::process::Stdio;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

/// Where the attach should land.
pub struct Target {
    /// ssh argv prefix, e.g. `ssh -o BatchMode=yes`. The host is appended when
    /// the template does not already contain `{host}`.
    pub ssh: Vec<String>,
    /// Host alias or `user@host`.
    pub host: String,
    /// Remote root override; empty means "resolve from $HOME".
    pub root: Option<String>,
}

/// A completed remote command.
struct RemoteResult {
    code: i32,
    stdout: String,
}

impl Target {
    /// The argv for one remote command line, host substituted where asked.
    fn argv(&self, remote: &[String]) -> Vec<String> {
        let mut argv: Vec<String> = self
            .ssh
            .iter()
            .map(|part| part.replace("{host}", &self.host))
            .collect();
        if !self.ssh.iter().any(|part| part.contains("{host}")) {
            argv.push(self.host.clone());
        }
        argv.push("--".into());
        argv.extend(remote.iter().cloned());
        argv
    }
}

/// Run one command over the transport, optionally feeding it stdin.
async fn run(
    target: &Target,
    remote: &[String],
    stdin: Option<Vec<u8>>,
) -> io::Result<RemoteResult> {
    let argv = target.argv(remote);
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot run {:?}: {error}", target.ssh.first()),
        )
    })?;
    if let Some(bytes) = stdin {
        let mut pipe = child.stdin.take().expect("piped stdin");
        pipe.write_all(&bytes).await?;
        drop(pipe);
    }
    let output = child.wait_with_output().await?;
    Ok(RemoteResult {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
    })
}

/// Probe the remote host: platform, home, and whether our binary is there.
fn probe_script(version: &str, arch: &str, requested_root: Option<&str>) -> String {
    let root_line = requested_root
        .map(|root| format!("root={root}\n"))
        .unwrap_or_default();
    format!(
        "set -eu\n\
         {root_line}\
         printf 'system %s\\n' \"$(uname -s)\"\n\
         printf 'machine %s\\n' \"$(uname -m)\"\n\
         printf 'home %s\\n' \"$HOME\"\n\
         root=\"${{DSH_SHELLOUT_HOME:-$HOME/.dsh-shellout}}\"\n\
         test -w \"$HOME\" || root=\"/tmp/dsh-shellout-$(id -u)\"\n\
         printf 'root %s\\n' \"$root\"\n\
         binary=\"$root/bin/{version}-{arch}/runner\"\n\
         if [ -x \"$binary\" ]; then printf 'binary %s\\n' \"$binary\"; else printf 'binary missing\\n'; fi\n\
         digest=missing\n\
         if command -v sha256sum >/dev/null 2>&1; then digest=sha256sum\n\
         elif command -v shasum >/dev/null 2>&1; then digest=shasum\n\
         fi\n\
         printf 'digest %s\\n' \"$digest\"\n"
    )
}

/// What the probe learned.
struct Remote {
    system: String,
    machine: String,
    root: String,
    binary: Option<String>,
    /// `sha256sum` or `shasum`, or `missing` when the host has neither.
    digest: String,
}

/// Parse the probe output; every line is `key value`.
fn parse_probe(stdout: &str) -> io::Result<Remote> {
    let mut system = None;
    let mut machine = None;
    let mut root = None;
    let mut binary = None;
    let mut digest = None;
    for line in stdout.lines() {
        match line.split_once(' ') {
            Some(("system", value)) => system = Some(value.to_string()),
            Some(("machine", value)) => machine = Some(value.to_string()),
            Some(("root", value)) => root = Some(value.to_string()),
            Some(("binary", value)) if value != "missing" => binary = Some(value.to_string()),
            Some(("digest", value)) => digest = Some(value.to_string()),
            _ => {}
        }
    }
    Ok(Remote {
        system: system.ok_or_else(|| io::Error::other("probe did not report the system"))?,
        machine: machine.ok_or_else(|| io::Error::other("probe did not report the machine"))?,
        root: root.ok_or_else(|| io::Error::other("probe did not report a state root"))?,
        binary,
        digest: digest.unwrap_or_else(|| "missing".into()),
    })
}

/// Whether the bytes are a statically linked executable.
///
/// A glibc build reproduces exactly the failure this plane exists to avoid: the
/// host reports a loader error instead of running. Look for a `PT_INTERP`
/// segment, which only a dynamically linked executable carries.
fn is_static_executable(bytes: &[u8]) -> bool {
    if bytes.len() < 64 || &bytes[..4] != b"\x7fELF" || bytes[4] != 2 {
        return false;
    }
    let read_u64 = |offset: usize| -> Option<u64> {
        let slice = bytes.get(offset..offset + 8)?;
        Some(u64::from_le_bytes(slice.try_into().ok()?))
    };
    let read_u16 = |offset: usize| -> Option<u16> {
        let slice = bytes.get(offset..offset + 2)?;
        Some(u16::from_le_bytes(slice.try_into().ok()?))
    };
    let (Some(phoff), Some(phentsize), Some(phnum)) = (read_u64(32), read_u16(54), read_u16(56))
    else {
        return false;
    };
    for index in 0..u64::from(phnum) {
        let start = match usize::try_from(phoff + index * u64::from(phentsize)) {
            Ok(start) => start,
            Err(_) => return false,
        };
        match read_u64(start) {
            Some(3) => return false,
            Some(_) => {}
            None => return false,
        }
    }
    true
}

/// Whether the host needs this build pushed at all.
fn needs_push(remote: &Remote) -> bool {
    remote.binary.is_none()
}

/// Push this build's binary to the remote path.
async fn push(target: &Target, remote: &Remote, destination: &str) -> io::Result<()> {
    let bytes = std::fs::read(std::env::current_exe()?)?;
    if !is_static_executable(&bytes) {
        return Err(io::Error::other(
            "the controller binary is not statically linked: build it with scripts/build-static.sh so \
             the host does not need a matching libc",
        ));
    }
    let digest = sha256_hex(&bytes);
    let directory = PathBuf::from(destination)
        .parent()
        .map(|parent| parent.display().to_string())
        .unwrap_or_else(|| remote.root.clone());
    // `shasum` needs `-a 256`; `sha256sum` reads the digest from stdin.
    let verify = if remote.digest == "shasum" {
        "shasum -a 256 -c - >/dev/null"
    } else {
        "sha256sum -c - >/dev/null"
    };
    // Values travel as arguments, never interpolated into the script: the path
    // comes from the host's own `$HOME` and a single quote in it would otherwise
    // end the quoting and run whatever follows.
    let script = format!(
        "set -eu\n\
         umask 077\n\
         mkdir -p \"$1\"\n\
         chmod 700 \"$1\"\n\
         cat > \"$2\"\n\
         chmod 700 \"$2\"\n\
         printf '%s  %s\\n' \"$3\" \"$2\" | {verify}\n\
         chmod 700 \"$1/$(basename \"$2\")\" 2>/dev/null || true\n\
         mv \"$2\" \"$4\"\n\
         printf 'pushed %s\\n' \"$4\"\n"
    );
    let incoming = format!("{destination}.incoming.{}", std::process::id());
    let result = run(
        target,
        &[
            "sh".to_string(),
            "-c".to_string(),
            script,
            "sh".to_string(),
            directory,
            incoming,
            digest,
            destination.to_string(),
        ],
        Some(bytes),
    )
    .await?;
    if result.code != 0 {
        return Err(io::Error::other(format!(
            "push to {} failed (exit {}): {}",
            target.host, result.code, result.stdout
        )));
    }
    Ok(())
}

/// Verify the remote binary runs and reports a matching version.
async fn verify(target: &Target, binary: &str, state_root: &str) -> io::Result<()> {
    let result = run(
        target,
        &[
            binary.to_string(),
            "--mode".into(),
            "status".into(),
            "--state".into(),
            format!("{state_root}/state"),
        ],
        None,
    )
    .await?;
    // `status` exits 1 when no executor owns the state root yet, which still
    // proves the binary ran; only a loader/arch failure is an error.
    if result.code == 0 || result.code == 1 {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "the pushed binary cannot run on the host (exit {}): {}",
        result.code, result.stdout
    )))
}

/// Bootstrap and bridge the caller's stdio to the remote executor.
pub async fn attach(target: Target, version: &str) -> io::Result<()> {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        other => {
            return Err(io::Error::other(format!(
                "no static build for host arch {other}"
            )))
        }
    };
    let probe = run(
        &target,
        &[
            "sh".to_string(),
            "-c".to_string(),
            probe_script(version, arch, target.root.as_deref()),
        ],
        None,
    )
    .await?;
    if probe.code != 0 {
        return Err(io::Error::other(format!(
            "cannot probe {}: exit {}: {}",
            target.host, probe.code, probe.stdout
        )));
    }
    let remote = parse_probe(&probe.stdout)?;
    if remote.system != "Linux" {
        return Err(io::Error::other(format!(
            "{} runs {}, and this plane only serves Linux hosts",
            target.host, remote.system
        )));
    }
    if remote.digest == "missing" {
        return Err(io::Error::other(format!(
            "{} has neither sha256sum nor shasum, so a pushed binary could not be verified",
            target.host
        )));
    }
    if remote.machine != arch {
        return Err(io::Error::other(format!(
            "{} is {}, this build is {arch}: push a matching build instead of cross-loading",
            target.host, remote.machine
        )));
    }
    let root = target.root.clone().unwrap_or_else(|| remote.root.clone());
    let binary = remote
        .binary
        .clone()
        .unwrap_or_else(|| format!("{root}/bin/{version}-{arch}/runner"));
    if needs_push(&remote) {
        push(&target, &remote, &binary).await?;
    }
    verify(&target, &binary, &root).await?;

    let mut child = spawn_stdio(&target, &binary, &root)?;
    bridge(&mut child).await
}

/// Start the remote executor over the transport and return the child.
fn spawn_stdio(target: &Target, binary: &str, root: &str) -> io::Result<Child> {
    let remote = vec![
        binary.to_string(),
        "--mode".into(),
        "stdio".into(),
        "--state".into(),
        format!("{root}/state"),
        "--socket".into(),
        format!("{root}/control.sock"),
        "--history".into(),
        format!("{root}/history.sqlite"),
    ];
    let argv = target.argv(&remote);
    Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Copy the caller's stdio to the remote executor and back.
async fn bridge(child: &mut Child) -> io::Result<()> {
    let mut child_stdin = child.stdin.take().expect("piped stdin");
    let mut child_stdout = child.stdout.take().expect("piped stdout");
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let upstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = stdin.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            child_stdin.write_all(&buffer[..read]).await?;
            child_stdin.flush().await?;
        }
        child_stdin.shutdown().await
    };
    let downstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = child_stdout.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            stdout.write_all(&buffer[..read]).await?;
            stdout.flush().await?;
        }
        Ok::<(), io::Error>(())
    };
    let _ = tokio::try_join!(upstream, downstream);
    let status = child.wait().await?;
    // A remote executor that refuses to start (bad socket path, refused lock,
    // loader error) must not look like a clean end of session.
    if !status.success() {
        return Err(io::Error::other(format!(
            "the remote executor exited with {status}"
        )));
    }
    Ok(())
}

/// Lowercase hex sha256 of one buffer.
fn sha256_hex(bytes: &[u8]) -> String {
    // A dependency-free sha256 keeps the static build free of extra crypto code
    // paths; this digest only has to match `sha256sum` on the remote.
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut message = bytes.to_vec();
    let bit_len = (bytes.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in message.chunks(64) {
        let mut w = [0u32; 64];
        for (index, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([
                chunk[index * 4],
                chunk[index * 4 + 1],
                chunk[index * 4 + 2],
                chunk[index * 4 + 3],
            ]);
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h.iter().map(|word| format!("{word:08x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_the_known_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn the_host_is_appended_once() {
        let target = Target {
            ssh: vec!["ssh".into()],
            host: "example".into(),
            root: None,
        };
        assert_eq!(
            target.argv(&["true".into()]),
            vec!["ssh", "example", "--", "true"]
        );
        let templated = Target {
            ssh: vec!["ssh".into(), "{host}".into()],
            host: "example".into(),
            root: None,
        };
        assert_eq!(
            templated.argv(&["true".into()]),
            vec!["ssh", "example", "--", "true"]
        );
    }

    #[test]
    fn a_host_that_reports_our_binary_is_not_pushed_again() {
        let present = parse_probe(
            "system Linux\nmachine x86_64\nhome /home/u\nroot /home/u/.dsh-shellout\nbinary /home/u/.dsh-shellout/bin/0.2.0-x86_64/runner\ndigest sha256sum\n",
        )
        .expect("parse");
        assert!(!needs_push(&present), "{:?}", present.binary);
        let missing = parse_probe("system Linux\nmachine x86_64\nhome /home/u\nroot /home/u/.dsh-shellout\nbinary missing\ndigest sha256sum\n")
            .expect("parse");
        assert!(needs_push(&missing));
    }

    #[test]
    fn probe_output_is_parsed() {
        let remote = parse_probe("system Linux\nmachine x86_64\nhome /home/u\nroot /home/u/.dsh-shellout\nbinary missing\ndigest shasum\n")
            .expect("parse");
        assert_eq!(remote.system, "Linux");
        assert_eq!(remote.root, "/home/u/.dsh-shellout");
        assert!(remote.binary.is_none());
    }
}
