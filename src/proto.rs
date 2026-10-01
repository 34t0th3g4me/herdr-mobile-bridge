//! Wire protocol of the Herdr Mobile bridge.
//!
//! The Android app drives this protocol over an SSH exec channel. It is
//! newline-delimited JSON; every frame carries the same envelope.
//!
//! Envelope field order matters only for byte-for-byte comparison with the
//! reference build, not for the app, which parses JSON.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u8 = 1;

/// One protocol frame in either direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub version: u8,
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "sessionId")]
    pub session_id: Option<String>,
    pub timestamp: u64,
    /// Present on pushed events; absent on direct replies.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub payload: Value,
}

impl Frame {
    pub fn event(kind: &str, session_id: &str, seq: u64, payload: Value) -> Self {
        Frame {
            version: PROTOCOL_VERSION,
            id: format!("f-{}-{}", now_ms(), seq),
            kind: kind.to_string(),
            session_id: Some(session_id.to_string()),
            timestamp: now_ms(),
            seq: Some(seq),
            payload,
        }
    }

    pub fn reply(_id: &str, kind: &str, session_id: &str, payload: Value) -> Self {
        Frame {
            version: PROTOCOL_VERSION,
            id: format!("f-{}-{}", now_ms(), next_frame_counter()),
            kind: kind.to_string(),
            session_id: Some(session_id.to_string()),
            timestamp: now_ms(),
            seq: None,
            payload,
        }
    }

    pub fn error(id: &str, session_id: &str, code: &str, message: &str) -> Self {
        Frame::reply(
            id,
            "error",
            session_id,
            serde_json::json!({
                "code": code,
                "message": message,
                "requestId": id,
            }),
        )
    }
}

/// Milliseconds since the Unix epoch, matching the reference build's stamps.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn next_frame_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Client frames the app sends. Unknown kinds must be answered with
/// `unsupported` rather than dropped, so the app can surface the failure.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientFrame {
    #[serde(rename = "hello")]
    Hello { #[allow(dead_code)] payload: HelloPayload },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "session.list")]
    SessionList,
    #[serde(rename = "snapshot.get")]
    SnapshotGet { #[allow(dead_code)] payload: SnapshotGetPayload },
    #[serde(rename = "raw.read")]
    RawRead { payload: RawReadPayload },
    #[serde(rename = "resume")]
    Resume { #[allow(dead_code)] payload: Value },
    #[serde(rename = "timeline.subscribe")]
    TimelineSubscribe { payload: TimelineSubscribePayload },
    #[serde(rename = "pane.sendText")]
    PaneSendText { payload: SendTextPayload },
    #[serde(rename = "pane.sendKeys")]
    PaneSendKeys { payload: SendKeysPayload },
    #[serde(rename = "pane.close")]
    PaneClose { payload: WritePayload },
    #[serde(rename = "agent.focus")]
    AgentFocus { payload: WritePayload },
    #[serde(rename = "agent.interrupt")]
    AgentInterrupt { payload: WritePayload },
    #[serde(rename = "agent.prompt")]
    AgentPrompt { payload: AgentPromptPayload },
    #[serde(rename = "tab.create")]
    TabCreate { payload: TabCreatePayload },
    #[serde(rename = "tab.focus")]
    TabFocus { payload: WritePayload },
    #[serde(rename = "tab.close")]
    TabClose { payload: WritePayload },
    #[serde(rename = "space.close")]
    SpaceClose { payload: WritePayload },
    #[serde(rename = "space.focus")]
    SpaceFocus { payload: WritePayload },
    #[serde(rename = "approval.respond")]
    ApprovalRespond { #[allow(dead_code)] payload: Value },
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct HelloPayload {
    #[serde(default, rename = "clientName")]
    pub client_name: Option<String>,
    #[serde(default, rename = "clientVersion")]
    pub client_version: Option<String>,
    #[serde(default, rename = "lastSeq")]
    pub last_seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SnapshotGetPayload {
    #[serde(default, rename = "lastSeq")]
    pub last_seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RawReadPayload {
    #[serde(rename = "paneId")]
    pub pane_id: String,
    #[serde(default)]
    pub lines: Option<u32>,
    /// Herdr pane-read source; defaults to `recent` when absent.
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct TimelineSubscribePayload {
    /// Panes to stream terminal blocks for, as a list.
    #[serde(default, rename = "paneIds")]
    pub pane_ids: Vec<String>,
    /// Single-pane form some clients send.
    #[serde(default, rename = "paneId")]
    pub pane_id: Option<String>,
}

impl TimelineSubscribePayload {
    /// The panes to stream, accepting either `paneIds` or a single `paneId`.
    pub fn panes(&self) -> Vec<String> {
        if !self.pane_ids.is_empty() {
            self.pane_ids.clone()
        } else {
            self.pane_id.iter().cloned().collect()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WritePayload {
    #[serde(rename = "paneId", default)]
    pub pane_id: Option<String>,
    #[serde(rename = "tabId", default)]
    pub tab_id: Option<String>,
    #[serde(rename = "spaceId", default)]
    pub space_id: Option<String>,
    #[serde(rename = "idempotencyKey", default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendTextPayload {
    #[serde(rename = "paneId")]
    pub pane_id: String,
    pub text: String,
    #[serde(rename = "idempotencyKey", default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendKeysPayload {
    #[serde(rename = "paneId")]
    pub pane_id: String,
    pub keys: Value,
    #[serde(rename = "idempotencyKey", default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AgentPromptPayload {
    #[serde(rename = "paneId", default)]
    pub pane_id: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(rename = "idempotencyKey", default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TabCreatePayload {
    #[serde(rename = "spaceId", default)]
    pub space_id: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(rename = "idempotencyKey", default)]
    pub idempotency_key: Option<String>,
}
