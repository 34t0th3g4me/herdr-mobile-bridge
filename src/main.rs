//! `herdr-mobile-bridge` — mobile bridge for a Herdr server.
//!
//! Modes:
//!   * `stdio --session <name>` — what the mobile app runs over SSH.
//!   * `serve --listen <addr>`  — WebSocket listener for direct LAN use.
//!   * `sessions` / `doctor`    — discovery and diagnostics.
//!   * `pair` / `revoke`        — device-token lifecycle for `serve`.

mod config;
mod core;
mod doctor;
mod herdr;
mod proto;
mod serve;
mod session;
mod stdio;

use serde_json::json;

const USAGE: &str = "\
herdr-mobile-bridge — bridge a Herdr server to the Herdr Mobile app.

Usage:
  herdr-mobile-bridge stdio   [--session <name>]
  herdr-mobile-bridge serve   [--listen <addr>] [--session <name>]
  herdr-mobile-bridge sessions [--json]
  herdr-mobile-bridge doctor   [--json]
  herdr-mobile-bridge pair
  herdr-mobile-bridge revoke
  herdr-mobile-bridge --version
";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprint!("{USAGE}");
        std::process::exit(2);
    }

    let command = args[0].as_str();
    let rest = &args[1..];
    match command {
        "--version" | "-V" | "version" => {
            println!("herdr-mobile-bridge {}", env!("CARGO_PKG_VERSION"));
        }
        "--help" | "-h" | "help" => print!("{USAGE}"),
        "stdio" => {
            let session = match session_flag(rest) {
                Ok(s) => s,
                Err(msg) => {
                    eprintln!("herdr-mobile-bridge: {msg}");
                    std::process::exit(2);
                }
            };
            let socket = config::session_socket(&session);
            if !socket.exists() {
                eprintln!(
                    "herdr-mobile-bridge: session `{session}` socket not found at {}",
                    socket.display()
                );
                std::process::exit(1);
            }
            if let Err(e) = stdio::run(&socket.to_string_lossy(), &session) {
                eprintln!("herdr-mobile-bridge: {e}");
                std::process::exit(1);
            }
        }
        "serve" => {
            let session = match session_flag(rest) {
                Ok(s) => s,
                Err(msg) => {
                    eprintln!("herdr-mobile-bridge: {msg}");
                    std::process::exit(2);
                }
            };
            let listen = flag(rest, "--listen").unwrap_or_else(|| "127.0.0.1:8756".to_string());
            let socket = config::session_socket(&session);
            if let Err(e) = serve::run(&listen, &socket.to_string_lossy(), &session) {
                eprintln!("herdr-mobile-bridge: {e}");
                std::process::exit(1);
            }
        }
        "sessions" => {
            let as_json = rest.iter().any(|a| a == "--json");
            let list: Vec<_> = config::list_sessions()
                .into_iter()
                .map(|name| {
                    let socket = config::session_socket(&name);
                    json!({
                        "name": name,
                        "socketPath": socket.to_string_lossy(),
                        "reachable": config::reachable(&socket),
                    })
                })
                .collect();
            if as_json {
                println!("{}", serde_json::to_string_pretty(&list).unwrap());
            } else {
                for entry in &list {
                    let marker = if entry["reachable"].as_bool().unwrap_or(false) {
                        "●"
                    } else {
                        "○"
                    };
                    println!(
                        "{marker}  {}  {}",
                        entry["name"].as_str().unwrap_or(""),
                        entry["socketPath"].as_str().unwrap_or("")
                    );
                }
            }
        }
        "doctor" => doctor::run(rest.iter().any(|a| a == "--json")),
        "pair" => {
            let role = match flag(rest, "--role").as_deref() {
                Some("read") => serve::Role::Read,
                Some("admin") => serve::Role::Admin,
                Some("control") | None => serve::Role::Control,
                Some(other) => {
                    eprintln!("herdr-mobile-bridge: invalid role `{other}` (read|control|admin)");
                    std::process::exit(2);
                }
            };
            match serve::TokenStore::new().issue(role) {
                Ok((record, token)) => {
                    println!("Device ID: {}", record.id);
                    println!("Role:      {}", record.role);
                    println!("Token:     {token}");
                    println!("The token is shown once; store it in the device secure store.");
                }
                Err(e) => {
                    eprintln!("herdr-mobile-bridge: {e}");
                    std::process::exit(1);
                }
            }
        }
        "revoke" => {
            let Some(device_id) = flag(rest, "--device-id") else {
                eprintln!("herdr-mobile-bridge: revoke requires --device-id <id>");
                std::process::exit(2);
            };
            match serve::TokenStore::new().revoke(&device_id) {
                Ok(true) => println!("device {device_id} revoked"),
                Ok(false) => println!("no device with id {device_id}"),
                Err(e) => {
                    eprintln!("herdr-mobile-bridge: {e}");
                    std::process::exit(1);
                }
            }
        }
        other => {
            eprintln!("herdr-mobile-bridge: unknown command `{other}`\n");
            eprint!("{USAGE}");
            std::process::exit(2);
        }
    }
}

/// Read `--flag value` from the argument tail.
fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

/// A `--session` value that is safe to interpolate into a socket path: a plain
/// name, never a path. This keeps an operator typo or a hostile caller from
/// pointing the bridge at an arbitrary `herdr.sock`.
fn session_flag(args: &[String]) -> Result<String, String> {
    let name = flag(args, "--session").unwrap_or_else(|| "default".to_string());
    let ok = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if ok {
        Ok(name)
    } else {
        Err(format!("invalid session name `{name}` (letters, digits, -_. only)"))
    }
}
