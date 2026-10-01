//! `doctor`: report environment facts the app and a human can act on.

use crate::config;
use serde_json::json;

/// Print the diagnostics as `--json` or human-readable text.
pub fn run(as_json: bool) {
    let config_dir = config::config_dir();
    let default_socket = config::session_socket("default");
    let token_store = crate::serve::TokenStore::new();

    let report = json!({
        "bridge_version": env!("CARGO_PKG_VERSION"),
        "config_dir_exists": config_dir.is_dir(),
        "default_listen": "127.0.0.1:8756",
        "default_socket_exists": default_socket.exists(),
        "paired_devices": token_store.active().len(),
        "herdr_binary_found": config::herdr_binary().is_some(),
        "mobile_protocol": crate::proto::PROTOCOL_VERSION,
        "allowed_origins_configured": std::env::var_os("HERDR_MOBILE_ALLOWED_ORIGINS").is_some(),
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
    println!("paired devices:    {}", token_store.active().len());
}
