//! `serve`: a WebSocket companion server for the mobile app (direct LAN use).
//!
//! Authentication mirrors the reference bridge:
//!
//! * paired **device tokens** live as JSON Lines in
//!   `~/.config/herdr-mobile-bridge/tokens.jsonl`, each with a role
//!   (`read` | `control` | `admin`); only the token's SHA-256 is stored;
//! * a request must present a matching token, compared in constant time;
//! * with **no** token configured, only loopback peers are served (as `admin`),
//!   so an unpaired server cannot be exposed over a LAN or VPN by accident.

use crate::session::Session;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::{IpAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tungstenite::error::Error as WsError;
use tungstenite::handshake::server::{Callback, Request, ServerHandshake};
use tungstenite::http::{Response as HttpResponse, StatusCode};
use tungstenite::Message;

/// Max concurrent WebSocket clients; bounds threads and Herdr subscriptions.
const MAX_CLIENTS: usize = 32;

/// How long a peer has to complete the WebSocket handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A paired device, as stored in `tokens.jsonl`. Only the SHA-256 of the token
/// is kept, so the store cannot leak a usable credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub id: String,
    /// SHA-256 of the device token, lowercase hex.
    pub hash: String,
    pub role: String,
    #[serde(default)]
    pub revoked: bool,
}

/// SHA-256 of a token, lowercase hex — the only form ever persisted.
fn token_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    hex::encode(h.finalize())
}

/// Role a connection is allowed to act as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Read,
    Control,
    Admin,
}

impl Role {
    /// Unknown roles fail closed to read-only.
    fn parse(s: &str) -> Role {
        match s {
            "admin" => Role::Admin,
            "control" => Role::Control,
            _ => Role::Read,
        }
    }

    /// Whether this role may issue mutating frames.
    fn can_write(self) -> bool {
        matches!(self, Role::Control | Role::Admin)
    }
}

/// The paired-device store.
pub struct TokenStore {
    path: PathBuf,
}

impl TokenStore {
    pub fn new() -> Self {
        TokenStore {
            path: crate::config::config_dir()
                .join("..")
                .join("herdr-mobile-bridge")
                .join("tokens.jsonl"),
        }
    }

    /// Every stored device, including revoked ones.
    pub fn load(&self) -> Vec<TokenRecord> {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return Vec::new();
        };
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    /// Devices that may still authenticate.
    pub fn active(&self) -> Vec<TokenRecord> {
        self.load().into_iter().filter(|r| !r.revoked).collect()
    }

    /// Create a device token and store only its hash. Returns the record and
    /// the one-time plaintext token, which is never written to disk.
    pub fn issue(&self, role: Role) -> std::io::Result<(TokenRecord, String)> {
        let token = new_token();
        let record = TokenRecord {
            id: format!(
                "{}-{}-{}-{}",
                rand_hex(8),
                rand_hex(4),
                rand_hex(4),
                rand_hex(12)
            ),
            hash: token_hash(&token),
            role: match role {
                Role::Read => "read",
                Role::Control => "control",
                Role::Admin => "admin",
            }
            .to_string(),
            revoked: false,
        };
        self.append(&record)?;
        Ok((record, token))
    }

    fn append(&self, record: &TokenRecord) -> std::io::Result<()> {
        use std::io::Write;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, record).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// Mark a device revoked. The record is kept (as the reference does), so
    /// the id cannot be silently reused.
    pub fn revoke(&self, device_id: &str) -> std::io::Result<bool> {
        let mut records = self.load();
        let mut found = false;
        for r in records.iter_mut() {
            if r.id == device_id {
                r.revoked = true;
                found = true;
            }
        }
        if !found {
            return Ok(false);
        }
        let body: String = records
            .iter()
            .filter_map(|r| serde_json::to_string(r).ok())
            .map(|l| l + "\n")
            .collect();
        std::fs::write(&self.path, body)?;
        Ok(true)
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A 32-byte random token as lowercase hex.
fn new_token() -> String {
    use rand::Rng;
    rand::thread_rng()
        .sample_iter(&rand::distributions::Standard)
        .take(32)
        .map(|b: u8| format!("{b:02x}"))
        .collect()
}

/// `n` random lowercase alphanumeric characters.
fn rand_hex(n: usize) -> String {
    use rand::Rng;
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(n)
        .map(char::from)
        .collect::<String>()
        .to_lowercase()
}

/// Constant-time byte comparison, so token checks do not leak a prefix.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A 403 handshake rejection: `ErrorResponse::new` would default to HTTP 200,
/// which tungstenite refuses to send.
fn reject(reason: &str) -> HttpResponse<Option<String>> {
    HttpResponse::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Some(reason.to_string()))
        .expect("static response builds")
}

/// Serve WebSocket clients on `listen`, bridging each to `socket_path`.
pub fn run(listen: &str, socket_path: &str, session_name: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen)?;
    let tokens = Arc::new(TokenStore::new().active());
    let origins = allowed_origins();
    if tokens.is_empty() {
        eprintln!(
            "warning: no device tokens configured: only loopback peers are served \
             (run `herdr-mobile-bridge pair` to add a device)"
        );
    }
    if !is_loopback_listen(listen) {
        eprintln!("warning: binding {listen} exposes the bridge beyond loopback");
    }
    eprintln!("herdr-mobile-bridge serve listening on {listen} session={session_name}");

    let live = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        if live.load(Ordering::Relaxed) >= MAX_CLIENTS {
            let _ = stream.shutdown(std::net::Shutdown::Both);
            continue;
        }
        let _ = stream.set_write_timeout(Some(HANDSHAKE_TIMEOUT));
        let _ = stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
        let peer = stream.peer_addr().ok().map(|a| a.ip());
        let tokens = Arc::clone(&tokens);
        let origins = Arc::clone(&origins);
        let live = Arc::clone(&live);
        let socket_path = socket_path.to_string();
        let session_name = session_name.to_string();
        live.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name("ws-client".into())
            .spawn(move || {
                if let Err(e) =
                    handle_client(stream, &socket_path, &session_name, peer, &tokens, &origins)
                {
                    eprintln!("ws client ended: {e}");
                }
                live.fetch_sub(1, Ordering::Relaxed);
            })?;
    }
    Ok(())
}

/// Whether `host:port` binds a loopback address only.
fn is_loopback_listen(listen: &str) -> bool {
    let host = listen
        .rsplit_once(':')
        .map(|(h, _)| h.trim_matches(|c| c == '[' || c == ']'))
        .unwrap_or(listen);
    matches!(host, "localhost" | "127.0.0.1" | "::1")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Origins permitted for a browser-initiated WebSocket, mirroring
/// `HERDR_MOBILE_ALLOWED_ORIGINS` (comma-separated). Empty means no browser
/// origin is allowed; native apps send no `Origin` and are unaffected.
fn allowed_origins() -> Arc<Vec<String>> {
    Arc::new(
        std::env::var("HERDR_MOBILE_ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

fn handle_client(
    stream: std::net::TcpStream,
    socket_path: &str,
    session_name: &str,
    peer: Option<IpAddr>,
    tokens: &[TokenRecord],
    origins: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let loopback = matches!(peer, Some(ip) if ip.is_loopback());
    let all_tokens = tokens.to_vec();
    let origins = origins.to_vec();
    let role = Arc::new(Mutex::new(Role::Read));

    let role_writer = Arc::clone(&role);
    let for_check = all_tokens.clone();
    let callback = move |req: &Request, resp: tungstenite::handshake::server::Response| {
        // Reject browser origins outright: WebSockets bypass the same-origin
        // policy, so an unlisted page must not reach the protocol.
        if let Some(origin) = req.headers().get("origin").and_then(|v| v.to_str().ok()) {
            if !origins.iter().any(|a| a == origin) {
                return Err(reject("origin not allowed"));
            }
        }
        let supplied = req
            .uri()
            .query()
            .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("token=")))
            .unwrap_or_default()
            .to_string();
        let supplied_hash = token_hash(&supplied);
        let matched = for_check
            .iter()
            .find(|r| !r.revoked && ct_eq(r.hash.as_bytes(), supplied_hash.as_bytes()));
        if let Some(record) = matched {
            *role_writer.lock() = Role::parse(&record.role);
            return Ok(resp);
        }
        if all_tokens.is_empty() && loopback {
            *role_writer.lock() = Role::Admin;
            return Ok(resp);
        }
        Err(reject("unauthorized"))
    };

    let mut ws = handshake_with_deadline(stream, callback, Instant::now() + HANDSHAKE_TIMEOUT)?;
    let role = *role.lock();
    let role_name = match role {
        Role::Read => "read",
        Role::Control => "control",
        Role::Admin => "admin",
    };
    let session = Session::connect_as(socket_path, session_name, role_name)?;

    loop {
        session.pump_events();
        for frame in session.drain_out() {
            ws.send(Message::Text(serde_json::to_string(&frame)?))?;
        }
        let incoming = match ws.read() {
            Ok(Message::Text(text)) => Some(text.to_string()),
            Ok(Message::Binary(bytes)) => Some(String::from_utf8_lossy(&bytes).into_owned()),
            Ok(Message::Ping(p)) => {
                let _ = ws.send(Message::Pong(p));
                None
            }
            Ok(Message::Close(_)) | Err(WsError::ConnectionClosed) => break,
            Ok(_) => None,
            Err(WsError::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                None
            }
            Err(e) => return Err(e.into()),
        };
        let Some(text) = incoming else { continue };

        // A read-only device may not mutate; everything else is allowed.
        if !role.can_write() && is_write_frame(&text) {
            ws.send(Message::Text(serde_json::to_string(&json!({
                "version": 1,
                "id": "server",
                "type": "error",
                "sessionId": session_name,
                "timestamp": 0,
                "payload": {
                    "code": "capability_denied",
                    "message": "Control role required",
                },
            }))?))?;
            continue;
        }
        session.handle_line(&text);
        for frame in session.drain_out() {
            ws.send(Message::Text(serde_json::to_string(&frame)?))?;
        }
    }
    Ok(())
}

/// Complete the server handshake, but never wait longer than `deadline` for a
/// peer that connects and sends nothing. A socket read timeout alone does not
/// abort `accept_hdr`; tungstenite reports that as `Interrupted`, so the
/// handshake is driven explicitly against the deadline.
fn handshake_with_deadline<C>(
    stream: std::net::TcpStream,
    callback: C,
    deadline: Instant,
) -> Result<tungstenite::WebSocket<std::net::TcpStream>, Box<dyn std::error::Error>>
where
    C: Callback,
{
    use tungstenite::handshake::HandshakeError;
    let mut result = ServerHandshake::start(stream, callback, None).handshake();
    loop {
        match result {
            Ok(ws) => return Ok(ws),
            Err(HandshakeError::Interrupted(mid)) => {
                if Instant::now() >= deadline {
                    return Err("handshake timed out".into());
                }
                result = mid.handshake();
            }
            Err(HandshakeError::Failure(e)) => return Err(e.into()),
        }
    }
}

/// Frame types that change state, used for read-only enforcement.
fn is_write_frame(text: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return false;
    };
    matches!(
        v.get("type").and_then(|t| t.as_str()),
        Some(
            "pane.sendText"
                | "pane.sendKeys"
                | "pane.close"
                | "agent.focus"
                | "agent.interrupt"
                | "agent.prompt"
                | "tab.create"
                | "tab.focus"
                | "tab.close"
                | "space.close"
                | "space.focus"
        )
    )
}
