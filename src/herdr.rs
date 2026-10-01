//! Client for a Herdr server's unix-socket JSON-RPC API.
//!
//! Two connection kinds, because Herdr treats them differently:
//!
//! * A **request** connection serves exactly one call and is then closed by
//!   the server. Every [`HerdrClient::call`] therefore opens its own socket.
//! * An **event** connection is created by `events.subscribe` and stays open,
//!   streaming pushed events until it is dropped.
//!
//! Mixing the two on one socket makes the server reset the connection, so the
//! split is a hard requirement, not a preference.

use parking_lot::Mutex;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

/// How long a single request or the subscribe handshake may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A Herdr server endpoint. Cheap to clone; holds one event stream at most.
pub struct HerdrClient {
    socket_path: String,
    next_id: AtomicU64,
    events: Mutex<Option<Receiver<Value>>>,
    /// Keeps the subscribed socket alive for as long as this client exists.
    event_stream: Mutex<Option<UnixStream>>,
}

impl HerdrClient {
    /// Record the endpoint. Does not open a connection yet.
    pub fn connect(socket_path: &str) -> std::io::Result<Self> {
        // Fail fast when the socket is missing, as the reference build does.
        UnixStream::connect(socket_path)?;
        Ok(HerdrClient {
            socket_path: socket_path.to_string(),
            next_id: AtomicU64::new(1),
            events: Mutex::new(None),
            event_stream: Mutex::new(None),
        })
    }

    fn open(&self) -> Result<UnixStream, HerdrError> {
        let stream = UnixStream::connect(&self.socket_path).map_err(HerdrError::Transport)?;
        stream
            .set_read_timeout(Some(CALL_TIMEOUT))
            .map_err(HerdrError::Transport)?;
        Ok(stream)
    }

    /// One request on a fresh connection; the server closes it afterwards.
    pub fn call(&self, method: &str, params: Value) -> Result<Value, HerdrError> {
        let id = format!("bridge-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut stream = self.open()?;

        let request = json!({ "id": id, "method": method, "params": params });
        let mut line = serde_json::to_vec(&request).map_err(HerdrError::Encode)?;
        line.push(b'\n');
        stream.write_all(&line).map_err(HerdrError::Transport)?;
        stream.flush().map_err(HerdrError::Transport)?;

        let mut reader = BufReader::new(stream);
        self.read_reply(&mut reader, &id)
    }

    fn read_reply(
        &self,
        reader: &mut BufReader<UnixStream>,
        id: &str,
    ) -> Result<Value, HerdrError> {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => return Err(HerdrError::Closed),
                Ok(_) => {}
                Err(e) => return Err(HerdrError::Transport(e)),
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            // Ignore anything that is not our reply (e.g. early events).
            if value.get("id").and_then(Value::as_str) != Some(id) {
                continue;
            }
            if let Some(err) = value.get("error") {
                return Err(HerdrError::Server {
                    code: err
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    message: err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("herdr error")
                        .to_string(),
                });
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    /// `session.snapshot`, unwrapped: exactly the object the bridge re-frames.
    pub fn snapshot(&self) -> Result<Value, HerdrError> {
        let result = self.call("session.snapshot", json!({}))?;
        Ok(result.get("snapshot").cloned().unwrap_or(result))
    }

    /// `protocol` and `version` as reported by the server snapshot.
    pub fn server_versions(&self) -> (Option<u64>, Option<String>) {
        match self.snapshot() {
            Ok(snap) => (
                snap.get("protocol").and_then(Value::as_u64),
                snap
                    .get("version")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            ),
            Err(_) => (None, None),
        }
    }

    /// Subscribe on a dedicated, long-lived connection and stream events into
    /// an internal channel. Re-subscribing replaces the previous stream.
    pub fn subscribe(&self, subscriptions: Vec<Value>) -> Result<(), HerdrError> {
        let mut stream = self.open()?;
        stream
            .set_read_timeout(None)
            .map_err(HerdrError::Transport)?;

        let request = json!({
            "id": "bridge-subscribe",
            "method": "events.subscribe",
            "params": { "subscriptions": subscriptions },
        });
        let mut line = serde_json::to_vec(&request).map_err(HerdrError::Encode)?;
        line.push(b'\n');
        stream.write_all(&line).map_err(HerdrError::Transport)?;
        stream.flush().map_err(HerdrError::Transport)?;

        // The handshake reply shares the stream with the events that follow.
        let reader_stream = stream.try_clone().map_err(HerdrError::Transport)?;
        let (tx, rx) = mpsc::channel::<Value>();
        let mut reader = BufReader::new(reader_stream);
        std::thread::Builder::new()
            .name("herdr-events".into())
            .spawn(move || read_events(&mut reader, &tx))
            .map_err(HerdrError::Transport)?;

        *self.events.lock() = Some(rx);
        *self.event_stream.lock() = Some(stream);
        Ok(())
    }

    /// Drain one already-queued server message without blocking.
    pub fn try_event(&self) -> Option<Value> {
        self.events.lock().as_ref()?.try_recv().ok()
    }
}

/// Read until the socket closes, forwarding parsed event frames.
fn read_events(reader: &mut BufReader<UnixStream>, tx: &Sender<Value>) {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let is_event = value.get("event").is_some()
            || value.get("data").is_some()
            || value.get("result").is_none();
        if is_event && tx.send(value).is_err() {
            break;
        }
    }
}

/// Failure from a Herdr call, split the way the mobile protocol needs it.
#[derive(Debug)]
pub enum HerdrError {
    /// Herdr answered with a structured error.
    Server { code: String, message: String },
    /// Reader ended or the socket closed before a reply arrived.
    Closed,
    Transport(std::io::Error),
    Encode(serde_json::Error),
}

impl HerdrError {
    /// `code: message`, as the reference bridge surfaces it to the app.
    pub fn describe(&self) -> String {
        match self {
            HerdrError::Server { code, message } => format!("{code}: {message}"),
            HerdrError::Closed => "herdr socket closed".to_string(),
            HerdrError::Transport(e) => format!("transport error: {e}"),
            HerdrError::Encode(e) => format!("encode error: {e}"),
        }
    }

    /// `write_failed` when Herdr itself rejected the call, `herdr_unavailable`
    /// when the socket could not be used at all.
    pub fn mobile_code(&self) -> &'static str {
        match self {
            HerdrError::Server { .. } => "write_failed",
            _ => "herdr_unavailable",
        }
    }
}
