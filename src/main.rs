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
            let session = flag(rest, "--session").unwrap_or_else(|| "default".to_string());
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
            let session = flag(rest, "--session").unwrap_or_else(|| "default".to_string());
            let listen = flag(rest, "--listen").unwrap_or_else(|| "127.0.0.1:8756".to_string());
            let socket = config::session_socket(&session);
            let tokens = serve::TokenStore::new();
            let token = tokens.load();
            if let Err(e) = serve::run(&listen, &socket.to_string_lossy(), &session, token) {
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
            let tokens = serve::TokenStore::new();
            match tokens.generate() {
                Ok(token) => println!("{token}"),
                Err(e) => {
                    eprintln!("herdr-mobile-bridge: {e}");
                    std::process::exit(1);
                }
            }
        }
        "revoke" => {
            let path = config::config_dir().join("bridge-device-token");
            match std::fs::remove_file(path) {
                Ok(()) => println!("device token revoked"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    println!("no device token present")
                }
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
