//! The bridge session core: transport-agnostic, non-blocking frame handling.
//!
//! The session never performs a Herdr call on the calling thread. All requests
//! are dispatched to a small worker pool and every frame is pushed onto one
//! outbound queue, which the transport drains. This keeps the read loop
//! responsive: a slow `pane.read` for one pane cannot delay another pane's
//! delta, a write, or a snapshot.

use crate::core::{Bridge, ServerInfo};
use crate::herdr::{HerdrClient, HerdrError};
use crate::proto::{ClientFrame, Frame, WritePayload};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::Arc;
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

/// How long a transport waits for input before draining events.
pub const PUMP_INTERVAL: Duration = Duration::from_millis(10);

/// Worker threads serving Herdr calls (writes and pane reads).
const WORKERS: usize = 4;

/// Pending jobs cap; a flood sheds work instead of growing memory.
const JOB_QUEUE: usize = 1024;

/// At most one delta per pane per window. The reference bridge streams a pane's
/// terminal timeline at roughly this cadence (~0.2/s); matching it keeps a
/// continuously repainting TUI from flooding the client and starving other
/// frames.
const DELTA_MIN_MS: u64 = 5000;

/// Mutable per-connection state, shared with the workers.
struct State {
    /// Write idempotency: key → result (`Null` while a job is in flight).
    seen: HashMap<String, Value>,
    order: VecDeque<String>,
    subscribed: bool,
    /// Panes whose terminal timeline the client subscribed to.
    timeline_panes: HashSet<String>,
    /// Last time a delta was sent per pane (rate floor).
    last_delta_ms: HashMap<String, u64>,
    /// Last pane revision whose content was streamed; a delta is sent only when
    /// the revision actually moves, which keeps a busy TUI from flooding the
    /// client with identical viewports.
    last_revision: HashMap<String, u64>,
}

impl State {
    fn remember(&mut self, key: &str, result: Value) {
        const MAX_REMEMBERED: usize = 512;
        if !self.seen.contains_key(key) && self.seen.len() >= MAX_REMEMBERED {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
        if self.seen.insert(key.to_string(), result).is_none() {
            self.order.push_back(key.to_string());
        }
    }
}

/// Work handed to a worker thread.
enum Job {
    Write {
        id: String,
        session: String,
        kind: String,
        payload: Value,
        key: String,
    },
    Delta {
        pane: String,
    },
}

/// One bridge session bound to a single Herdr server socket.
pub struct Session {
    bridge: Arc<Bridge>,
    state: Arc<Mutex<State>>,
    out_tx: Sender<Frame>,
    out_rx: Mutex<Option<Receiver<Frame>>>,
    jobs: SyncSender<Job>,
}

impl Session {
    pub fn connect(socket_path: &str, session_name: &str) -> std::io::Result<Self> {
        Self::connect_as(socket_path, session_name, "admin")
    }

    /// Connect with an explicit advertised role (`admin` for the SSH transport,
    /// where the OS account already gates access).
    pub fn connect_as(socket_path: &str, session_name: &str, role: &str) -> std::io::Result<Self> {
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
        let bridge = Arc::new(Bridge::new(client, info));
        let state = Arc::new(Mutex::new(State {
            seen: HashMap::new(),
            order: VecDeque::new(),
            subscribed: false,
            timeline_panes: HashSet::new(),
            last_delta_ms: HashMap::new(),
            last_revision: HashMap::new(),
        }));
        let (out_tx, out_rx) = mpsc::channel::<Frame>();
        let (job_tx, job_rx) = mpsc::sync_channel::<Job>(JOB_QUEUE);
        let job_rx = Arc::new(Mutex::new(job_rx));

        for i in 0..WORKERS {
            let bridge = Arc::clone(&bridge);
            let state = Arc::clone(&state);
            let out = out_tx.clone();
            let jobs = Arc::clone(&job_rx);
            std::thread::Builder::new()
                .name(format!("herdr-worker-{i}"))
                .spawn(move || worker(bridge, state, out, jobs))?;
        }

        Ok(Session {
            bridge,
            state,
            out_tx,
            out_rx: Mutex::new(Some(out_rx)),
            jobs: job_tx,
        })
    }

    pub fn session_name(&self) -> &str {
        self.bridge.session_name()
    }

    /// Frames produced since the last call (from workers and event projection).
    /// Only usable while the receiver is still owned by the session.
    pub fn drain_out(&self) -> Vec<Frame> {
        let mut frames = Vec::new();
        if let Some(rx) = self.out_rx.lock().as_ref() {
            while let Ok(frame) = rx.try_recv() {
                frames.push(frame);
            }
        }
        frames
    }

    /// Move the outbound receiver to a dedicated writer thread, so frames are
    /// written the instant they are produced instead of on the next poll tick.
    pub fn take_out_receiver(&self) -> Option<Receiver<Frame>> {
        self.out_rx.lock().take()
    }

    /// Project pushed Herdr events and queue timeline deltas. Never blocks on a
    /// Herdr call; the pane reads happen on the worker pool.
    pub fn pump_events(&self) {
        // pane -> newest revision seen this pump
        let mut changed: Vec<(String, Option<u64>)> = Vec::new();
        while let Some(raw) = self.bridge.client().try_event() {
            if let Some((pane, rev)) = pane_output_changed(&raw) {
                match changed.iter_mut().find(|(p, _)| *p == pane) {
                    Some(entry) => entry.1 = rev.or(entry.1),
                    None => changed.push((pane, rev)),
                }
            }
            for frame in self.bridge.project_event(&raw) {
                let _ = self.out_tx.send(frame);
            }
        }
        if changed.is_empty() {
            return;
        }
        let now = crate::proto::now_ms();
        let mut todo = Vec::new();
        {
            let mut st = self.state.lock();
            for (pane, rev) in changed {
                if !st.timeline_panes.contains(&pane) {
                    continue;
                }
                // Skip only when the content is provably unchanged. An event
                // without a revision is always streamed.
                if let Some(rev) = rev {
                    if st.last_revision.get(&pane) == Some(&rev) {
                        continue;
                    }
                }
                if let Some(last) = st.last_delta_ms.get(&pane) {
                    if now.saturating_sub(*last) < DELTA_MIN_MS {
                        continue;
                    }
                }
                if let Some(rev) = rev {
                    st.last_revision.insert(pane.clone(), rev);
                }
                st.last_delta_ms.insert(pane.clone(), now);
                todo.push(pane);
            }
        }
        for pane in todo {
            let _ = self.jobs.try_send(Job::Delta { pane });
        }
    }

    /// Handle one inbound client line: emit immediate replies and dispatch any
    /// blocking Herdr work to the worker pool.
    pub fn handle_line(&self, line: &str) {
        let raw: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return,
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
                self.emit(Frame::error(
                    &id,
                    &session,
                    code,
                    &format!("unsupported frame type: {kind}"),
                ));
                return;
            }
        };

        match frame {
            ClientFrame::Hello { .. } => {
                let need_sub = !self.state.lock().subscribed;
                if need_sub {
                    let subs = SUBSCRIPTIONS.iter().map(|t| json!({ "type": t })).collect();
                    if self.bridge.client().subscribe(subs).is_ok() {
                        self.state.lock().subscribed = true;
                    }
                }
                let payload = self.bridge.hello_payload();
                self.emit(Frame::reply(&id, "hello.result", &session, payload));
            }
            ClientFrame::Ping => self.emit(Frame::reply(&id, "pong", &session, json!({}))),
            ClientFrame::SessionList => self.emit(Frame::reply(
                &id,
                "session.list.result",
                &session,
                json!({ "sessions": [{ "name": session, "reachable": true, "socketPath": "" }] }),
            )),
            ClientFrame::SnapshotGet { .. } => {
                let bridge = Arc::clone(&self.bridge);
                let out = self.out_tx.clone();
                spawn_blocking(move || match bridge.snapshot() {
                    Ok(frame) => {
                        let _ = out.send(frame);
                    }
                    Err(e) => {
                        let _ = out.send(error_with_code(&id, &session, "snapshot_failed", &e));
                    }
                });
            }
            ClientFrame::RawRead { payload } => {
                let bridge = Arc::clone(&self.bridge);
                let out = self.out_tx.clone();
                let pane = payload.pane_id.clone();
                let lines = payload.lines;
                let source = payload.source.clone();
                spawn_blocking(move || {
                    match bridge.raw_read(&pane, lines, source.as_deref()) {
                        Ok(mut result) => {
                            result["requestId"] = json!(id);
                            let _ = out.send(Frame::reply(&id, "raw.read.result", &session, result));
                        }
                        Err(e) => {
                            let _ = out.send(error_with_code(&id, &session, "read_failed", &e));
                        }
                    }
                });
            }
            // `resume` is answered with nothing.
            ClientFrame::Resume { .. } => {}
            ClientFrame::TimelineSubscribe { payload } => {
                let panes = payload.panes();
                if panes.is_empty() {
                    self.emit(Frame::reply(
                        &id,
                        "ack",
                        &session,
                        json!({ "duplicate": false, "requestId": id }),
                    ));
                    return;
                }
                self.state.lock().timeline_panes.extend(panes.iter().cloned());
                self.emit(Frame::reply(
                    &id,
                    "ack",
                    &session,
                    json!({ "duplicate": false, "requestId": id }),
                ));
                let bridge = Arc::clone(&self.bridge);
                let out = self.out_tx.clone();
                spawn_blocking(move || {
                    let batch = bridge.timeline_batch(&panes);
                    let seq = bridge.next_seq_public();
                    let _ = out.send(Frame::event("timeline.batch", &session, seq, batch));
                });
            }
            ClientFrame::ApprovalRespond { .. } => self.emit(Frame::error(
                &id,
                &session,
                "capability_degraded",
                "Herdr exposes no verified semantic approval response; use labeled pane keys only after explicit confirmation",
            )),
            ClientFrame::Unsupported => self.emit(Frame::error(
                &id,
                &session,
                "unsupported",
                "unsupported frame type",
            )),
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
            | ClientFrame::SpaceFocus { .. }) => {
                self.handle_write(&id, &session, &write)
            }
        }
    }

    fn handle_write(&self, id: &str, session: &str, frame: &ClientFrame) {
        let (kind, payload, key) = write_parts(frame);
        let Some(key) = key else {
            self.emit(Frame::error(
                id,
                session,
                "idempotency_required",
                "write requires idempotencyKey",
            ));
            return;
        };

        {
            let mut st = self.state.lock();
            if st.seen.contains_key(&key) {
                self.emit(Frame::reply(
                    id,
                    "ack",
                    session,
                    json!({ "duplicate": true, "requestId": id }),
                ));
                return;
            }
            // Reserve the key so a concurrent duplicate is recognised while the
            // write is still in flight.
            st.remember(&key, Value::Null);
        }

        self.emit(Frame::reply(
            id,
            "ack",
            session,
            json!({ "duplicate": false, "requestId": id }),
        ));
        let job = Job::Write {
            id: id.to_string(),
            session: session.to_string(),
            kind,
            payload,
            key,
        };
        if self.jobs.try_send(job).is_err() {
            self.emit(Frame::error(
                id,
                session,
                "write_failed",
                "bridge is busy; retry with the same idempotencyKey",
            ));
        }
    }

    fn emit(&self, frame: Frame) {
        let _ = self.out_tx.send(frame);
    }
}

/// A worker loop: the only place that performs blocking Herdr calls.
fn worker(
    bridge: Arc<Bridge>,
    state: Arc<Mutex<State>>,
    out: Sender<Frame>,
    jobs: Arc<Mutex<Receiver<Job>>>,
) {
    loop {
        // Hold the lock only to take a job, never while doing the work.
        let Ok(job) = jobs.lock().recv() else { break };
        match job {
            Job::Write {
                id,
                session,
                kind,
                payload,
                key,
            } => match bridge.write(&kind, &payload) {
                Ok(result) => {
                    state.lock().remember(&key, result);
                    let _ = out.send(Frame::reply(
                        &id,
                        "command.result",
                        &session,
                        json!({ "ok": true, "requestId": id }),
                    ));
                }
                Err(e) => {
                    let _ = out.send(error_for(&id, &session, &e));
                }
            },
            Job::Delta { pane } => {
                if let Some(event) =
                    crate::core::timeline_event_for(bridge.client(), bridge.session_name(), &pane)
                {
                    let seq = bridge.next_seq_public();
                    let _ = out.send(Frame::event(
                        "timeline.delta",
                        bridge.session_name(),
                        seq,
                        json!({ "event": event }),
                    ));
                }
            }
        }
    }
}

/// Run a blocking body on a short-lived thread. Used for one-off calls that are
/// not part of the bounded worker pool (snapshots, raw reads, batches).
fn spawn_blocking<F: FnOnce() + Send + 'static>(body: F) {
    let _ = std::thread::Builder::new()
        .name("herdr-call".into())
        .spawn(body);
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
        ClientFrame::PaneClose { payload } => (
            "pane.close".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::AgentFocus { payload } => (
            "agent.focus".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::AgentInterrupt { payload } => (
            "agent.interrupt".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::TabFocus { payload } => (
            "tab.focus".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::TabClose { payload } => (
            "tab.close".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::SpaceClose { payload } => (
            "space.close".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
        ClientFrame::SpaceFocus { payload } => (
            "space.focus".into(),
            target(payload),
            payload.idempotency_key.clone(),
        ),
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

/// The pane id (and content revision when present) from a
/// `pane_output_changed`/`pane.updated` server event.
fn pane_output_changed(raw: &Value) -> Option<(String, Option<u64>)> {
    let data = raw.get("data")?;
    let event = data.get("type").and_then(Value::as_str)?;
    match event {
        "pane_output_changed" => {
            let pane = data.get("pane_id").and_then(Value::as_str)?.to_string();
            Some((pane, data.get("revision").and_then(Value::as_u64)))
        }
        // Output changes also arrive as a full `pane_updated`.
        "pane_updated" => {
            let pane = data.get("pane")?;
            let id = pane.get("pane_id").and_then(Value::as_str)?.to_string();
            Some((id, pane.get("revision").and_then(Value::as_u64)))
        }
        _ => None,
    }
}
