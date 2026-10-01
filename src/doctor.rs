//! `doctor`: report environment facts the app and a human can act on.

use crate::config;
use serde_json::json;

/// Print the diagnostics as `--json` or human-readable text.
pub fn run(as_json: bool) {
    let config_dir = config::config_dir();
    let default_socket = config::session_socket("default");
    let token = config::config_dir().join("bridge-device-token");
    let audit_dir = config::config_dir().join("audit");

    let report = json!({
        "audit": {
            "configured": audit_dir.is_dir(),
            "directory_exists": audit_dir.is_dir(),
        },
        "bridge_version": env!("CARGO_PKG_VERSION"),
        "config_dir_exists": config_dir.is_dir(),
        "default_listen": "127.0.0.1:8756",
        "default_socket_exists": default_socket.exists(),
        "device_token_store_present": token.is_file(),
        "herdr_binary_found": config::herdr_binary().is_some(),
        "history_limit": "2048",
        "mobile_protocol": crate::proto::PROTOCOL_VERSION,
        "session_allowlist_configured": std::env::var_os("HERDR_BRIDGE_SESSION_ALLOWLIST").is_some(),
    });

    if as_json {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        return;
    }

    println!("bridge version:     {}", env!("CARGO_PKG_VERSION"));
    println!(
        "herdr binary:      {}",
        if config::herdr_binary().is_some() {
            "found"
        } else {
            "missing"
        }
    );
    println!(
        "config dir:        {}",
        if config_dir.is_dir() { "ok" } else { "missing" }
    );
    println!(
        "default socket:    {}",
        if default_socket.exists() { "ok" } else { "missing" }
    );
}
