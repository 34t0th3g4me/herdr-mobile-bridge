//! Herdr ⇄ mobile projection: state, snapshot, writes and event translation.
//!
//! Everything here was derived by running the reference bridge (0.3.1) against
//! a live Herdr server and recording every frame in both directions; see
//! `README.md` for the recorded fixtures.

use crate::herdr::{HerdrClient, HerdrError};
use crate::proto::{Frame, PROTOCOL_VERSION};
use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Herdr emits `pane_updated` at spinner cadence — ~10 times a second per pane,
/// each with a different terminal title and otherwise identical state. The
/// reference bridge forwards every one of them, so the client repaints its whole
/// agent list ten times a second and never gets to settle. Collapse those to
/// edges: one `agent.status` per pane per real change (status, title, agent),
/// with a slow keepalive for a client that missed the edge.
pub const AGENT_STATUS_MIN_MS: u64 = 2000;

/// Last `agent.status` state advertised per pane, used to suppress the spinner
/// churn described on [`AGENT_STATUS_MIN_MS`].
#[derive(Debug, Clone)]
pub struct AgentStatusState {
    /// Stable identity of the advert: pane, status, agent and title.
    pub key: String,
    pub last_ms: u64,
}

/// Capability list advertised in `hello.result`. Matches the reference build.
pub const CAPABILITIES: &[&str] = &[
    "resume",
    "audit",
    "snapshot",
    "events",
    "timeline",
    "pane.read",
    "pane.sendText",
    "pane.sendKeys",
    "agent.prompt",
    "focus",
    "interrupt",
    "close",
    "tab.create",
    "notification.hints",
];

/// Versions we could re-derive from a live handshake or trust from the build.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub bridge_version: String,
    pub herdr_protocol: u64,
    pub herdr_version: String,
    pub hostname: String,
    pub session: String,
    /// Role advertised in `hello.result`; enforcement happens per frame.
    pub role: String,
}

impl ServerInfo {
    pub fn server_id(&self) -> String {
        format!("{}:{}", self.hostname, self.session)
    }
}

/// Per-connection bridge state: the socket plus the monotonic event sequence.
/// Shared fields are `Arc`-backed so the socket client and sequence counter can
/// be used from worker threads while the session keeps handling requests.
pub struct Bridge {
    client: HerdrClient,
    info: Arc<ServerInfo>,
    seq: Arc<Mutex<u64>>,
    /// Pane id → last advertised status, for spinner coalescing.
    agent_status: Mutex<HashMap<String, AgentStatusState>>,
}

/// Whether an `agent.status` advert is worth emitting: on a real change of the
/// advertised identity, or on the keepalive interval. Spinner churn (same
/// identity) is dropped. Pure so the policy is unit-testable.
fn agent_status_due(prev: Option<&AgentStatusState>, key: &str, now: u64) -> bool {
    match prev {
        Some(prev) => prev.key != key || now.saturating_sub(prev.last_ms) >= AGENT_STATUS_MIN_MS,
        None => true,
    }
}

/// Remove spinner glyphs from a title and collapse whitespace, so the braille
/// frame a live pane rewrites ten times a second does not read as a change.
/// Spinner frames are not information: the client animates its own indicator
/// from `agentStatus`.
fn strip_spinner(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    let mut last_space = false;
    for ch in title.chars() {
        let spinner = matches!(ch,
            '\u{2800}'..='\u{28FF}'            // braille spinners ⠋⠙⠹…
            | '◐' | '◓' | '◑' | '◒'            // half-circle spinners
            | '✳' | '✢' | '✱' | '✲' | '✶' | '✽'  // asterisk spinners
            | '⏳' | '⌛');                     // hourglasses
        if spinner {
            continue;
        }
        if ch.is_whitespace() {
            if last_space {
                continue;
            }
            last_space = true;
            out.push(' ');
        } else {
            last_space = false;
            out.push(ch);
        }
    }
    out.trim().to_string()
}

impl Bridge {
    pub fn new(client: HerdrClient, info: ServerInfo) -> Self {
        Bridge {
            client,
            info: Arc::new(info),
            seq: Arc::new(Mutex::new(0)),
            agent_status: Mutex::new(HashMap::new()),
        }
    }

    pub fn session_name(&self) -> &str {
        &self.info.session
    }

    pub fn client(&self) -> &HerdrClient {
        &self.client
    }

    #[allow(dead_code)]
    fn next_seq(&self) -> u64 {
        self.next_seq_public()
    }

    /// Next event sequence number; safe to call from any thread.
    pub fn next_seq_public(&self) -> u64 {
        let mut s = self.seq.lock();
        *s += 1;
        *s
    }

    /// `hello.result` payload the app expects after its `hello`.
    pub fn hello_payload(&self) -> Value {
        json!({
            "authRequired": false,
            "protocol": {
                "degraded": ["approval.respond"],
                "role": self.info.role,
                "selected": PROTOCOL_VERSION,
                "supported": [PROTOCOL_VERSION],
            },
            "server": {
                "bridgeVersion": self.info.bridge_version,
                "capabilities": CAPABILITIES,
                "herdrProtocol": self.info.herdr_protocol,
                "herdrVersion": self.info.herdr_version,
                "hostname": self.info.hostname,
                "serverId": self.info.server_id(),
            },
        })
    }

    /// `snapshot.get` → a `snapshot` frame whose payload nests Herdr's own
    /// `session.snapshot` verbatim under `snapshot`.
    pub fn snapshot(&self) -> Result<Frame, HerdrError> {
        let snap = self.client.snapshot()?;
        let seq = self.next_seq();
        Ok(Frame::event(
            "snapshot",
            &self.info.session,
            seq,
            json!({ "snapshot": snap }),
        ))
    }

    /// `raw.read` → `pane.read` on Herdr, reshaped to `raw.read.result`.
    pub fn raw_read(
        &self,
        pane_id: &str,
        lines: Option<u32>,
        source: Option<&str>,
    ) -> Result<Value, HerdrError> {
        let mut params = Map::new();
        params.insert("pane_id".into(), json!(pane_id));
        params.insert("source".into(), json!(source.unwrap_or("recent")));
        if let Some(l) = lines {
            params.insert("lines".into(), json!(l));
        }
        let result = self.client.call("pane.read", Value::Object(params))?;
        let read = result.get("read").cloned().unwrap_or(Value::Null);
        Ok(json!({
            "paneId": pane_id,
            "text": read.get("text").cloned().unwrap_or(Value::Null),
            "revision": read.get("revision").cloned().unwrap_or(Value::Null),
            "truncated": read.get("truncated").cloned().unwrap_or(Value::Bool(false)),
        }))
    }

    /// Build a `timeline.batch` payload for the requested panes: the app paints
    /// each pane's TUI from these terminal blocks, keyed by pane id.
    pub fn timeline_batch(&self, pane_ids: &[String]) -> Value {
        let events: Vec<Value> = pane_ids
            .iter()
            .filter_map(|pane| self.timeline_event(pane))
            .collect();
        json!({
            "events": events,
            "paneId": pane_ids.first().cloned().unwrap_or_default(),
        })
    }

    /// One synthetic terminal event for a pane, from its current viewport.
    pub fn timeline_event(&self, pane_id: &str) -> Option<Value> {
        timeline_event_for(&self.client, &self.info.session, pane_id)
    }

    /// Apply a mobile write frame and return the Herdr result.
    pub fn write(&self, kind: &str, payload: &Value) -> Result<Value, HerdrError> {
        let pane = || payload.get("paneId").and_then(Value::as_str);
        match kind {
            "pane.sendText" => self.client.call(
                "pane.send_text",
                json!({ "pane_id": pane(), "text": payload.get("text").cloned().unwrap_or(Value::Null) }),
            ),
            "pane.sendKeys" => {
                let keys = normalize_keys(payload.get("keys"));
                self.client
                    .call("pane.send_keys", json!({ "pane_id": pane(), "keys": keys }))
            }
            "agent.prompt" => self.client.call(
                "agent.prompt",
                json!({
                    "target": pane(),
                    "text": payload.get("text").cloned().unwrap_or(Value::Null),
                    "wait": false,
                }),
            ),
            "agent.interrupt" => self
                .client
                .call("pane.send_keys", json!({ "pane_id": pane(), "keys": ["ctrl+c"] })),
            "agent.focus" => self.client.call("agent.focus", json!({ "target": pane() })),
            "pane.close" => self.client.call("pane.close", json!({ "pane_id": pane() })),
            "tab.create" => self.client.call(
                "tab.create",
                json!({
                    "workspace_id": payload.get("spaceId").cloned().unwrap_or(Value::Null),
                    "label": payload.get("label").cloned().unwrap_or(Value::Null),
                    "cwd": payload.get("cwd").cloned().unwrap_or(Value::Null),
                    "focus": true,
                }),
            ),
            "tab.focus" => self.client.call(
                "tab.focus",
                json!({ "tab_id": payload.get("tabId").cloned().unwrap_or(Value::Null) }),
            ),
            "tab.close" => self.client.call(
                "tab.close",
                json!({ "tab_id": payload.get("tabId").cloned().unwrap_or(Value::Null) }),
            ),
            "space.focus" => self.client.call(
                "workspace.focus",
                json!({ "workspace_id": payload.get("spaceId").cloned().unwrap_or(Value::Null) }),
            ),
            "space.close" => self.client.call(
                "workspace.close",
                json!({
                    "workspace_id": payload.get("spaceId").cloned().unwrap_or(Value::Null),
                    "close_group": true,
                }),
            ),
            other => Err(HerdrError::Server {
                code: "unsupported".into(),
                message: format!("unsupported write frame type: {other}"),
            }),
        }
    }

    /// Translate one pushed Herdr event into zero or more mobile frames.
    pub fn project_event(&self, raw: &Value) -> Vec<Frame> {
        let Some(data) = raw.get("data") else {
            return Vec::new();
        };
        let event = data
            .get("type")
            .or_else(|| data.get("event"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut out = Vec::new();

        // Agent transitions arrive both as their own event and embedded in a
        // full `pane_updated`; the app keys off the pane, so both project to an
        // `agent.status` frame.
        if event == "pane_agent_detected" {
            if let Some(frame) = self.project_agent_status(data) {
                out.push(frame);
            }
        } else if event == "pane_created" {
            // A freshly created pane is announced with an `unknown` status even
            // before any agent is detected.
            if let Some(pane) = data.get("pane") {
                if let Some(frame) = self.project_agent_status(pane) {
                    out.push(frame);
                }
            }
        } else if event == "pane_updated" {
            // Only panes that actually own an agent produce a status frame;
            // the reference build stays quiet for plain terminals.
            if let Some(pane) = data.get("pane") {
                if pane.get("agent").is_some() || pane.get("agent_session").is_some() {
                    if let Some(frame) = self.project_agent_status(pane) {
                        out.push(frame);
                    }
                }
            }
        } else if event.starts_with("workspace_")
            && event.ends_with("_updated")
            && data.get("agent_status").is_some()
        {
            if let Some(frame) = self.project_agent_status(data) {
                out.push(frame);
            }
        }

        // Resource frames. `pane_updated` is a high-frequency output event and
        // is deliberately *not* surfaced as `resource.updated`; the reference
        // build only emits layout/workspace/tab/pane create-close-focus here.
        let action = if event == "pane_created" {
            Some("created")
        } else if event == "tab_created" {
            Some("created")
        } else if event == "workspace_created" {
            Some("created")
        } else if event == "pane_closed" || event == "tab_closed" || event == "workspace_closed" {
            Some("removed")
        } else if event == "pane_focused" {
            Some("focused")
        } else if event == "tab_focused" {
            Some("focused")
        } else if event == "workspace_focused" {
            Some("focused")
        } else if event == "layout_updated"
            || event == "workspace_updated"
            || event == "workspace_metadata_updated"
            || event == "workspace_renamed"
            || event == "workspace_moved"
            || event == "tab_renamed"
            || event == "tab_moved"
            || event == "pane_moved"
            || event == "pane_exited"
        {
            Some("updated")
        } else {
            None
        };
        if let Some(action) = action {
            out.push(self.resource_frame(action, data));
        }
        out
    }

    /// A coalesced `agent.status`. The pane's live spinner rewrites its own
    /// terminal title ~10 times a second, which the reference bridge relays
    /// verbatim; that alone starves the client. Emit instead only when the
    /// advertised identity (pane, status, agent, title) really changes, plus a
    /// periodic keepalive so a client that missed an edge still converges.
    /// Returns `None` for pure spinner churn, so no `seq` is consumed.
    fn project_agent_status(&self, data: &Value) -> Option<Frame> {
        let pane = data
            .get("pane_id")
            .and_then(Value::as_str)
            .or_else(|| data.get("workspace_id").and_then(Value::as_str))
            .or_else(|| data.get("tab_id").and_then(Value::as_str))
            .unwrap_or("")
            .to_string();
        let key = format!(
            "{}\u{1}{}\u{1}{}\u{1}{}",
            pane,
            data.get("agent_status").and_then(Value::as_str).unwrap_or(""),
            data.get("agent").and_then(Value::as_str).unwrap_or(""),
            strip_spinner(
                &["terminal_title_stripped", "terminal_title", "title"]
                    .iter()
                    .filter_map(|k| data.get(*k))
                    .find(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            ),
        );
        let now = crate::proto::now_ms();
        {
            let mut seen = self.agent_status.lock();
            if !agent_status_due(seen.get(&pane), &key, now) {
                return None;
            }
            seen.insert(
                pane,
                AgentStatusState {
                    key,
                    last_ms: now,
                },
            );
        }
        Some(self.agent_status_frame(data))
    }

    fn agent_status_frame(&self, data: &Value) -> Frame {
        let seq = self.next_seq();
        // The pane's display title is its stripped terminal title; a pane with
        // no title yet (a fresh shell) falls back to its working directory, as
        // the reference build does.
        let title = ["terminal_title_stripped", "terminal_title", "title"]
            .iter()
            .filter_map(|k| data.get(*k))
            .find(|v| v.as_str().is_some_and(|s| !s.is_empty()))
            .cloned()
            .or_else(|| data.get("cwd").cloned())
            .unwrap_or(Value::Null);
        let payload = json!({
            "agent": data.get("agent").cloned().unwrap_or(Value::Null),
            "agentStatus": data.get("agent_status").cloned().unwrap_or(Value::Null),
            "displayAgent": data.get("display_agent").cloned().unwrap_or(Value::Null),
            "paneId": data.get("pane_id").cloned().unwrap_or(Value::Null),
            "stateLabels": data.get("state_labels").cloned().unwrap_or(Value::Null),
            "tabId": data.get("tab_id").cloned().unwrap_or(Value::Null),
            "title": title,
            "workspaceId": data.get("workspace_id").cloned().unwrap_or(Value::Null),
        });
        Frame::event("agent.status", &self.info.session, seq, payload)
    }

    fn resource_frame(&self, action: &str, data: &Value) -> Frame {
        // `kind` is the entity family; `resource` is Herdr's own object. Close
        // events carry a trimmed object; rewrap it so consumers see the same
        // fields as the create path.
        let (kind, entity) = if let Some(w) = data.get("workspace") {
            ("workspace", w.clone())
        } else if let Some(t) = data.get("tab") {
            ("tab", t.clone())
        } else if let Some(p) = data.get("pane") {
            ("pane", p.clone())
        } else if let Some(l) = data.get("layout") {
            ("layout", l.clone())
        } else if data.get("tab_id").is_some() {
            ("tab", data.clone())
        } else if data.get("pane_id").is_some() && data.get("workspace_id").is_some() {
            ("pane", data.clone())
        } else if data.get("workspace_id").is_some() {
            ("workspace", data.clone())
        } else {
            ("unknown", Value::Null)
        };
        let event_name = data
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let seq = self.next_seq();
        let payload = json!({
            "event": event_name,
            "kind": kind,
            "resource": entity,
        });
        let frame_kind = match action {
            "created" => "resource.created",
            "removed" => "resource.removed",
            "focused" => "resource.focused",
            _ => "resource.updated",
        };
        Frame::event(frame_kind, &self.info.session, seq, payload)
    }
}

/// Herdr's `pane.send_keys` takes an array of key names; the app may send a
/// single string or an array.
fn normalize_keys(keys: Option<&Value>) -> Value {
    match keys {
        Some(Value::String(s)) => json!([s]),
        Some(Value::Array(_)) => keys.cloned().unwrap(),
        _ => json!([]),
    }
}

/// Build one synthetic `terminal` timeline event for a pane. Free function so a
/// worker thread can render a delta without holding the session lock.
pub fn timeline_event_for(client: &HerdrClient, session: &str, pane_id: &str) -> Option<Value> {
    let result = client
        .call(
            "pane.read",
            serde_json::json!({ "pane_id": pane_id, "source": "recent" }),
        )
        .ok()?;
    let text = result
        .get("read")
        .and_then(|r| r.get("text"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let timestamp = crate::proto::now_ms();
    Some(serde_json::json!({
        "adapter": "unknown",
        "blocks": [{ "text": text, "truncated": false, "type": "terminal" }],
        "confidence": "heuristic",
        "id": format!("terminal-{pane_id}-{timestamp}"),
        "kind": "terminal",
        "paneId": pane_id,
        "seq": timestamp,
        "serverId": "",
        "sessionId": session,
        "source": "terminal_fallback",
        "spaceId": "",
        "tabId": "",
        "timestamp": timestamp,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(key: &str, last_ms: u64) -> AgentStatusState {
        AgentStatusState { key: key.into(), last_ms }
    }

    #[test]
    fn first_frame_always_emitted() {
        assert!(agent_status_due(None, "w0:p1\u{1}working\u{1}omp\u{1}x", 1));
    }

    #[test]
    fn spinner_churn_is_dropped() {
        // Same normalized identity (spinner already stripped) within the window.
        let prev = st("w0:p1\u{1}working\u{1}omp\u{1}\u{3c0} title", 1000);
        assert!(!agent_status_due(Some(&prev), &prev.key, 1000 + AGENT_STATUS_MIN_MS - 1));
    }

    #[test]
    fn keepalive_re_emits_after_window() {
        let prev = st("w0:p1\u{1}working\u{1}omp\u{1}x", 1000);
        assert!(agent_status_due(Some(&prev), &prev.key, 1000 + AGENT_STATUS_MIN_MS));
    }

    #[test]
    fn real_change_is_immediate() {
        let prev = st("w0:p1\u{1}working\u{1}omp\u{1}x", 1000);
        // Any identity change — status, title, agent or pane — passes at once.
        assert!(agent_status_due(Some(&prev), "w0:p1\u{1}blocked\u{1}omp\u{1}x", 1001));
        assert!(agent_status_due(Some(&prev), "w0:p1\u{1}working\u{1}omp\u{1}build failed", 1001));
    }

    #[test]
    fn spinner_glyphs_are_stripped() {
        assert_eq!(strip_spinner("\u{3c0} \u{2807} title"), "\u{3c0} title");
        assert_eq!(strip_spinner("\u{3c0} \u{2819} title"), "\u{3c0} title");
        assert_eq!(strip_spinner("\u{3c0}   \u{2819}   title"), "\u{3c0} title");
        assert_eq!(strip_spinner("plain title"), "plain title");
    }

    #[test]
    fn a_real_title_change_still_emits() {
        let a = strip_spinner("\u{3c0} \u{2807} build");
        let b = strip_spinner("\u{3c0} \u{2807} test");
        assert_ne!(a, b);
    }
}
