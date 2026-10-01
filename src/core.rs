//! Herdr ⇄ mobile projection: state, snapshot, writes and event translation.
//!
//! Everything here was derived by running the reference bridge (0.3.1) against
//! a live Herdr server and recording every frame in both directions; see
//! `README.md` for the recorded fixtures.

use crate::herdr::{HerdrClient, HerdrError};
use crate::proto::{Frame, PROTOCOL_VERSION};
use serde_json::{json, Map, Value};

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
}

impl ServerInfo {
    pub fn server_id(&self) -> String {
        format!("{}:{}", self.hostname, self.session)
    }
}

/// Per-connection bridge state: the socket plus the monotonic event sequence.
pub struct Bridge {
    client: HerdrClient,
    info: ServerInfo,
    seq: u64,
}

impl Bridge {
    pub fn new(client: HerdrClient, info: ServerInfo) -> Self {
        Bridge { client, info, seq: 0 }
    }

    pub fn info(&self) -> &ServerInfo {
        &self.info
    }

    pub fn client(&self) -> &HerdrClient {
        &self.client
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// `hello.result` payload the app expects after its `hello`.
    pub fn hello_payload(&self) -> Value {
        json!({
            "authRequired": false,
            "protocol": {
                "degraded": ["approval.respond"],
                "role": "admin",
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
    pub fn snapshot(&mut self) -> Result<Frame, HerdrError> {
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
    pub fn raw_read(&self, pane_id: &str, lines: Option<u32>) -> Result<Value, HerdrError> {
        let mut params = Map::new();
        params.insert("pane_id".into(), json!(pane_id));
        params.insert("source".into(), json!("recent"));
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
    pub fn project_event(&mut self, raw: &Value) -> Vec<Frame> {
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
            out.push(self.agent_status_frame(data));
        } else if event == "pane_created" {
            // A freshly created pane is announced with an `unknown` status even
            // before any agent is detected.
            if let Some(pane) = data.get("pane") {
                out.push(self.agent_status_frame(pane));
            }
        } else if event == "pane_updated" {
            // Only panes that actually own an agent produce a status frame;
            // the reference build stays quiet for plain terminals.
            if let Some(pane) = data.get("pane") {
                if pane.get("agent").is_some() || pane.get("agent_session").is_some() {
                    out.push(self.agent_status_frame(pane));
                }
            }
        } else if event.starts_with("workspace_")
            && event.ends_with("_updated")
            && data.get("agent_status").is_some()
        {
            out.push(self.agent_status_frame(data));
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

    fn agent_status_frame(&mut self, data: &Value) -> Frame {
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

    fn resource_frame(&mut self, action: &str, data: &Value) -> Frame {
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
