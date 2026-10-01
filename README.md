# herdr-mobile-bridge (macOS / Linux)

The Herdr Mobile app ships its bridge only for Linux (x86_64). On macOS the app
therefore had no local bridge, so a Mac had to be reached through a bridge
running on another machine — and lost its bridge whenever that machine was off.

This crate is a wire-compatible reimplementation of the reference bridge, built
from the reference binary's own behaviour, so the app can run it natively on
macOS (arm64) as well as Linux.

## Modes

```
herdr-mobile-bridge stdio   [--session <name>]        # used by the app over SSH
herdr-mobile-bridge serve   [--listen <addr>] [--session <name>]
herdr-mobile-bridge sessions [--json]                 # discovered sessions
herdr-mobile-bridge doctor   [--json]                 # environment diagnostics
herdr-mobile-bridge pair     [--role read|control|admin]
herdr-mobile-bridge revoke   --device-id <id>
herdr-mobile-bridge --version                         # must print 0.3.1
```

`--version` prints `herdr-mobile-bridge 0.3.1` on purpose: the app compares the
installed version against the bundled one and would otherwise force a
re-upload.

## Install on macOS

```sh
cargo build --release
install -m755 target/release/herdr-mobile-bridge ~/.local/bin/herdr-mobile-bridge
```

The app probes `~/.local/bin/herdr-mobile-bridge`, `/usr/local/bin/...` and
`~/.cargo/bin/...` and runs `herdr-mobile-bridge stdio --session <name>` over
SSH exec. With an `sshd_config` `ForceCommand` dispatcher (POSIX-sh wrapper for
fish), that is the only wiring needed.

## Protocol notes

Recorded by running the reference binary against a live Herdr server and
diffing every frame in both directions.

* Envelope: `{version, id, type, sessionId, timestamp, payload}`; pushed events
  additionally carry a monotonic `seq`.
* `hello` → `hello.result`, then the bridge subscribes to Herdr events.
* `snapshot.get` → `snapshot` frame whose `payload.snapshot` is Herdr's
  `session.snapshot` object **verbatim**.
* `raw.read` → `raw.read.result` `{paneId, text, revision, truncated}`.
* Writes (`pane.sendText`, `pane.sendKeys`, `agent.prompt`, `agent.interrupt`,
  `agent.focus`, `pane.close`, `tab.create`, `tab.focus`, `tab.close`,
  `space.focus`, `space.close`) **require** an `idempotencyKey`; without one the
  bridge answers `error {code:"idempotency_required"}` and executes nothing.
  Otherwise a write replies with `ack` **first**, then `command.result`; a
  replayed key answers only `ack {duplicate:true}` and executes nothing again.
* Errors: `idempotency_required`, `write_failed`, `read_failed`,
  `snapshot_failed`, `herdr_unavailable`, `unsupported`, `capability_denied`,
  `capability_degraded`. `approval.respond` is always `capability_degraded` —
  Herdr protocol 22 has no verified semantic approval response.
* Events: `agent.status`, `resource.created|updated|removed|focused`.
* `raw.read` accepts `{paneId, lines?, source?}`; `source` defaults to `recent`.

### Herdr socket semantics (the subtle part)

A Herdr server connection is **single-purpose**:

* a request connection serves exactly one method and is then closed by the
  server;
* an event connection is created by `events.subscribe` and stays open,
  streaming events forever.

Mixing the two — issuing another request on a subscribed socket — makes the
server reset the connection. This client therefore opens one socket per call
and keeps a separate, dedicated socket for events.

Because `pane.agent_status_changed` and `pane.scroll_changed` require a
concrete `pane_id` (unknown for a workspace-wide app), agent transitions are
projected from `pane.updated`/`pane.agent_detected`/`pane_created` instead.

## Companion (`serve`) authentication

`serve` is for reaching a machine directly over a LAN or VPN without SSH.

* Devices are paired with `herdr-mobile-bridge pair [--role read|control|admin]`,
  which prints the token **once**. Only its SHA-256 is stored, in
  `~/.config/herdr-mobile-bridge/tokens.jsonl`.
* The client passes the token as `?token=<token>` on the WebSocket URL and is
  rejected with HTTP 403 before the upgrade when it does not match (constant-time
  comparison).
* With **no** paired device, only loopback peers are accepted (as `admin`);
  binding a non-loopback address without a token is warned about explicitly.
* `read` devices cannot issue write frames (`error {code:"capability_denied"}`).
* Browser origins are refused unless listed in `HERDR_MOBILE_ALLOWED_ORIGINS`
  (comma-separated); native clients send no `Origin` and are unaffected.

The transport is plain `ws://` — put it behind Tailscale/WireGuard, not the open
internet.

## Layout

| file | role |
|---|---|
| `proto.rs` | mobile frame envelope + client frame types |
| `herdr.rs` | Herdr unix-socket client (per-call sockets, event stream) |
| `core.rs` | state → snapshot; event → mobile frame projection |
| `session.rs` | transport-agnostic frame handling, idempotency |
| `stdio.rs` | SSH-exec transport |
| `serve.rs` | WebSocket transport + device token |
| `config.rs` | session discovery, socket paths |
| `doctor.rs` | diagnostics |
