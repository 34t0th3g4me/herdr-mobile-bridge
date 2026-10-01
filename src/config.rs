//! Session discovery and socket resolution.
//!
//! Herdr sessions live under `<config>/herdr/sessions/<name>/herdr.sock`, with
//! the unnamed `default` session at `<config>/herdr/herdr.sock`. This mirrors
//! the reference bridge so `sessions`/`doctor` and session selection agree.

use std::path::{Path, PathBuf};

/// `$XDG_CONFIG_HOME/herdr`, or `~/.config/herdr`.
pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("HERDR_CONFIG_DIR").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("herdr")
}

/// Socket path for a named session. `default` honours `HERDR_SOCKET_PATH`.
pub fn session_socket(session: &str) -> PathBuf {
    if session == "default" {
        if let Some(path) = std::env::var_os("HERDR_SOCKET_PATH").filter(|v| !v.is_empty()) {
            return PathBuf::from(path);
        }
        return config_dir().join("herdr.sock");
    }
    config_dir().join("sessions").join(session).join("herdr.sock")
}

/// Every known session: `default` plus each subdirectory of `sessions/`.
pub fn list_sessions() -> Vec<String> {
    let mut names = vec!["default".to_string()];
    let dir = config_dir().join("sessions");
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let mut found: Vec<String> = entries
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        found.sort();
        names.extend(found);
    }
    names
}

/// True when something is listening on the socket right now.
pub fn reachable(socket: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(socket).is_ok()
}

/// Where the herdr binary lives, if we can find it.
pub fn herdr_binary() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("HERDR_BIN").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    for dir in std::env::var("PATH").unwrap_or_default().split(':') {
        let candidate = Path::new(dir).join("herdr");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    for fallback in ["/usr/bin/herdr", "/usr/local/bin/herdr", "/opt/homebrew/bin/herdr"] {
        let candidate = PathBuf::from(fallback);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Machine hostname, reported in `hello.result.server.hostname`.
pub fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| {
            // No procfs (macOS): ask the system.
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}
