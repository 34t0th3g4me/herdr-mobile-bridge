//! The bridge session core: transport-agnostic frame handling.
//!
//! [`Session`] owns the Herdr connection and turns client frames into replies
//! and event frames. Transports (`stdio` over SSH, `serve` over WebSocket)
//! only move strings in and frames out.

use crate::core::{Bridge, ServerInfo};
use crate::herdr::{HerdrClient, HerdrError};
use crate::proto::{ClientFrame, Frame, WritePayload};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

/// Herdr events mirrored into mobile frames. `pane.agent_status_changed` and
/// `pane.scroll_changed` require a concrete `pane_id`, so agent transitions are
/// taken from `pane.updated`/`pane.agent_detected` instead.
const SUBSCRIPTIONS: &[&str] = &[
    "workspace.created",
    "workspace.updated",
    "workspace.metadata_updated",
    "workspace.renamed",
    "workspace.moved",
    "workspace.reordered",
    "workspace.closed",
    "workspace.focused",
    "tab.created",
    "tab.closed",
    "tab.focused",
    "tab.renamed",
    "tab.moved",
    "pane.created",
    "pane.closed",
    "pane.updated",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
    "layout.updated",
];

/// How long a transport waits for input before draining Herdr events.
pub const PUMP_INTERVAL: Duration = Duration::from_millis(25);

/// One bridge session bound to a single Herdr server socket.
pub struct Session {
    bridge: Bridge,
    /// Write idempotency: key → previous result, capped in [`Self::remember`].
    seen: HashMap<String, Value>,
    /// Insertion order of `seen`, used to evict the oldest key.
    order: std::collections::VecDeque<String>,
    subscribed: bool,
}

impl Session {
    /// Connect to `socket_path`; `session_name` labels the mobile session and
    /// `role` is reported in `hello.result` (defaults to `admin` for the SSH
    /// transport, where the OS account already gates access).
    pub fn connect(socket_path: &str, session_name: &str) -> std::io::Result<Self> {
        Self::connect_as(socket_path, session_name, "admin")
    }

    /// Connect with an explicit advertised role.
    pub fn connect_as(
        socket_path: &str,
        session_name: &str,
        role: &str,
    ) -> std::io::Result<Self> {
        let client = HerdrClient::connect(socket_path)?;
        let (protocol, version) = client.server_versions();
        let info = ServerInfo {
            bridge_version: env!("CARGO_PKG_VERSION").to_string(),
            herdr_protocol: protocol.unwrap_or(0),
            herdr_version: version.unwrap_or_else(|| "unknown".to_string()),
            hostname: crate::config::hostname(),
            session: session_name.to_string(),
            role: role.to_string(),
        };
        Ok(Session {
            bridge: Bridge::new(client, info),
            seen: HashMap::new(),
            order: std::collections::VecDeque::new(),
            subscribed: false,
        })
    }

    pub fn session_name(&self) -> &str {
        &self.bridge.info().session
    }

    /// Frames triggered by Herdr server pushes since the last call.
    pub fn pump_events(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(raw) = self.bridge.client().try_event() {
            frames.extend(self.bridge.project_event(&raw));
        }
        frames
    }

    /// Frames produced by one inbound client line.
    pub fn handle_line(&mut self, line: &str) -> Vec<Frame> {
        let raw: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return Vec::new(), // Unparseable input is ignored.
        };
        let id = raw
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let session = self.session_name().to_string();

        let frame: ClientFrame = match serde_json::from_value(raw.clone()) {
            Ok(f) => f,
            Err(_) => {
                let kind = raw.get("type").and_then(Value::as_str).unwrap_or("");
                let code = if kind == "approval.respond" {
                    "capability_degraded"
                } else {
                    "unsupported"
                };
                return vec![Frame::error(
                    &id,
                    &session,
                    code,
                    &format!("unsupported frame type: {kind}"),
                )];
            }
        };

        match frame {
            ClientFrame::Hello { .. } => {
                if !self.subscribed {
                    let subs = SUBSCRIPTIONS.iter().map(|t| json!({ "type": t })).collect();
                    // A failed subscription is non-fatal, but is retried on a
                    // later `hello` rather than latching the connection off.
                    if self.bridge.client().subscribe(subs).is_ok() {
                        self.subscribed = true;
                    }
                }
                let payload = self.bridge.hello_payload();
                vec![Frame::reply(&id, "hello.result", &session, payload)]
            }
            ClientFrame::Ping => vec![Frame::reply(&id, "pong", &session, json!({}))],
            ClientFrame::SessionList => vec![Frame::reply(
                &id,
                "session.list.result",
                &session,
                json!({ "sessions": [{ "name": session, "reachable": true, "socketPath": "" }] }),
            )],
            ClientFrame::SnapshotGet { .. } => match self.bridge.snapshot() {
                Ok(frame) => vec![frame],
                Err(e) => vec![error_with_code(&id, &session, "snapshot_failed", &e)],
            },
            ClientFrame::RawRead { payload } => {
                match self.bridge.raw_read(&payload.pane_id, payload.lines, payload.source.as_deref()) {
                    Ok(mut result) => {
                        result["requestId"] = json!(id);
                        vec![Frame::reply(&id, "raw.read.result", &session, result)]
                    }
                    // The reference build reports reads as `read_failed`, not
                    // `write_failed`, because the verb is not a write.
                    Err(e) => vec![error_with_code(&id, &session, "read_failed", &e)],
                }
            }
            // The reference acknowledges `timeline.subscribe` with a silent ack
            // (no payload) and answers `resume` with nothing at all.
            ClientFrame::Resume { .. } => Vec::new(),
            ClientFrame::TimelineSubscribe { .. } => vec![Frame::reply(
                &id,
                "ack",
                &session,
                json!({ "duplicate": false, "requestId": id }),
            )],
            ClientFrame::ApprovalRespond { .. } => vec![Frame::error(
                &id,
                &session,
                "capability_degraded",
                "Herdr exposes no verified semantic approval response; use labeled pane keys only after explicit confirmation",
            )],
            ClientFrame::Unsupported => vec![Frame::error(
                &id,
                &session,
                "unsupported",
                "unsupported frame type",
            )],
            write @ (ClientFrame::PaneSendText { .. }
            | ClientFrame::PaneSendKeys { .. }
            | ClientFrame::PaneClose { .. }
            | ClientFrame::AgentFocus { .. }
            | ClientFrame::AgentInterrupt { .. }
            | ClientFrame::AgentPrompt { .. }
            | ClientFrame::TabCreate { .. }
            | ClientFrame::TabFocus { .. }
            | ClientFrame::TabClose { .. }
            | ClientFrame::SpaceClose { .. }
            | ClientFrame::SpaceFocus { .. }) => self.handle_write(&id, &session, &write),
        }
    }

    fn handle_write(&mut self, id: &str, session: &str, frame: &ClientFrame) -> Vec<Frame> {
        let (kind, payload, key) = write_parts(frame);
        // Herdr replays are unsafe without a key, so the app must supply the
        // field; this is the reference bridge's `idempotency_required` contract.
        // An empty string still counts as present, matching the reference.
        let Some(key) = key else {
            return vec![Frame::error(
                id,
                session,
                "idempotency_required",
                "write requires idempotencyKey",
            )];
        };

        let duplicate = self.seen.contains_key(&key);
        let ack = Frame::reply(
            id,
            "ack",
            session,
            json!({ "duplicate": duplicate, "requestId": id }),
        );
        // A replayed write is acknowledged but produces no second result — the
        // reference bridge stops after the duplicate ack.
        if duplicate {
            return vec![ack];
        }
        let mut out = vec![ack];

        match self.bridge.write(&kind, &payload) {
            Ok(result) => self.remember(&key, result),
            Err(e) => {
                out.push(error_for(id, session, &e));
                return out;
            }
        }
        out.push(Frame::reply(
            id,
            "command.result",
            session,
            json!({ "ok": true, "requestId": id }),
        ));
        out
    }

    /// Remember a write result, bounding the cache so a long-lived connection
    /// cannot grow without limit. Older entries are evicted first.
    fn remember(&mut self, key: &str, result: Value) {
        const MAX_REMEMBERED: usize = 512;
        if self.seen.len() >= MAX_REMEMBERED {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
        if self.seen.insert(key.to_string(), result).is_none() {
            self.order.push_back(key.to_string());
        }
    }
}

/// Split a write frame into `(mobile kind, payload, idempotency key)`.
fn write_parts(frame: &ClientFrame) -> (String, Value, Option<String>) {
    let target = |p: &WritePayload| {
        json!({ "paneId": p.pane_id, "tabId": p.tab_id, "spaceId": p.space_id })
    };
    match frame {
        ClientFrame::PaneSendText { payload } => (
            "pane.sendText".into(),
            json!({ "paneId": payload.pane_id, "text": payload.text }),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::PaneSendKeys { payload } => (
            "pane.sendKeys".into(),
            json!({ "paneId": payload.pane_id, "keys": payload.keys }),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::AgentPrompt { payload } => (
            "agent.prompt".into(),
            json!({ "paneId": payload.pane_id, "text": payload.text }),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::TabCreate { payload } => (
            "tab.create".into(),
            json!({ "spaceId": payload.space_id, "label": payload.label, "cwd": payload.cwd }),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::PaneClose { payload } => {
            ("pane.close".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::AgentFocus { payload } => {
            ("agent.focus".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::AgentInterrupt { payload } => {
            ("agent.interrupt".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::TabFocus { payload } => {
            ("tab.focus".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::TabClose { payload } => {
            ("tab.close".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::SpaceClose { payload } => {
            ("space.close".into(), target(payload), payload.idempotency_key.clone())
        }
        ClientFrame::SpaceFocus { payload } => {
            ("space.focus".into(), target(payload), payload.idempotency_key.clone())
        }
        _ => ("unsupported".into(), json!({}), None),
    }
}

/// The mobile error frame for a Herdr failure.
pub fn error_for(id: &str, session: &str, e: &HerdrError) -> Frame {
    error_with_code(id, session, e.mobile_code(), e)
}

/// The mobile error frame with an explicit code (reads, snapshots, …).
pub fn error_with_code(id: &str, session: &str, code: &str, e: &HerdrError) -> Frame {
    Frame::error(
        id,
        session,
        code,
        &format!("herdr returned error {}", e.describe()),
    )
}
