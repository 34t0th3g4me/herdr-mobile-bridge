//! `serve`: a WebSocket listener for the mobile app (direct Wi-Fi / LAN use).
//!
//! The reference bridge serves `ws://127.0.0.1:<port>/?token=<device-token>`,
//! with the token checked before the WebSocket upgrade completes. Each
//! connection gets its own [`Session`] against the same Herdr server.

use crate::session::{Session, PUMP_INTERVAL};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tungstenite::{accept_hdr, handshake::server::Request, Message};

/// A paired device token stored on the serving host.
pub struct TokenStore {
    path: std::path::PathBuf,
}

impl TokenStore {
    pub fn new() -> Self {
        TokenStore {
            path: crate::config::config_dir().join("bridge-device-token"),
        }
    }

    pub fn exists(&self) -> bool {
        self.path.is_file()
    }

    pub fn load(&self) -> Option<String> {
        std::fs::read_to_string(&self.path)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    pub fn generate(&self) -> std::io::Result<String> {
        use rand::Rng;
        let token: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(48)
            .map(char::from)
            .collect();
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, &token)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(token)
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Serve WebSocket clients on `listen`, bridging each to `socket_path`.
pub fn run(
    listen: &str,
    socket_path: &str,
    session_name: &str,
    token: Option<String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen)?;
    let token = Arc::new(token);
    eprintln!(
        "herdr-mobile-bridge serve listening on {listen} session={session_name} auth={}",
        if token.is_some() { "token" } else { "open" }
    );

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let token = Arc::clone(&token);
        let socket_path = socket_path.to_string();
        let session_name = session_name.to_string();
        std::thread::Builder::new()
            .name("ws-client".into())
            .spawn(move || {
                if let Err(e) = handle_client(stream, &socket_path, &session_name, token.as_deref()) {
                    eprintln!("ws client ended: {e}");
                }
            })?;
    }
    Ok(())
}

fn handle_client(
    stream: std::net::TcpStream,
    socket_path: &str,
    session_name: &str,
    token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Reject before the upgrade completes when the token does not match.
    let expected = token.map(str::to_string);
    let check = move |req: &Request, resp: tungstenite::handshake::server::Response| {
        if let Some(expected) = &expected {
            let supplied = req
                .uri()
                .query()
                .and_then(|q| {
                    q.split('&')
                        .find_map(|kv| kv.strip_prefix("token="))
                })
                .unwrap_or_default();
            if supplied != expected {
                let err = tungstenite::handshake::server::ErrorResponse::new(Some(
                    "unauthorized".to_string(),
                ));
                return Err(err);
            }
        }
        Ok(resp)
    };

    let mut ws = accept_hdr(stream, check)?;
    let mut session = Session::connect(socket_path, session_name)?;
    ws.get_mut().set_read_timeout(Some(PUMP_INTERVAL))?;

    loop {
        for frame in session.pump_events() {
            ws.send(Message::Text(serde_json::to_string(&frame)?))?;
        }
        match ws.read() {
            Ok(Message::Text(text)) => {
                for frame in session.handle_line(text.as_str()) {
                    ws.send(Message::Text(serde_json::to_string(&frame)?))?;
                }
            }
            Ok(Message::Ping(p)) => {
                let _ = ws.send(Message::Pong(p));
            }
            Ok(Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => break,
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Unused today, kept so a future `serve` mode can negotiate subprotocols.
pub fn default_pump() -> Duration {
    PUMP_INTERVAL
}
