//! The SSH-exec (`stdio`) transport: newline-delimited JSON on stdin/stdout.
//!
//! The mobile app runs `herdr-mobile-bridge stdio --session <name>` over SSH.
//! A dedicated writer thread drains the session's outbound queue, so a reply is
//! written the moment a worker produces it — no per-tick latency.

use crate::proto::Frame;
use crate::session::{Session, PUMP_INTERVAL};
use std::io::{BufRead, Write};
use std::sync::mpsc::{self, RecvTimeoutError};

/// Run a stdio bridge session until the app closes stdin.
pub fn run(socket_path: &str, session_name: &str) -> std::io::Result<()> {
    let session = Session::connect(socket_path, session_name)?;

    // Writer thread: owns stdout and flushes every frame immediately.
    let out_rx = session
        .take_out_receiver()
        .ok_or_else(|| std::io::Error::other("outbound queue already taken"))?;
    let writer = std::thread::Builder::new()
        .name("stdout".into())
        .spawn(move || {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            while let Ok(frame) = out_rx.recv() {
                if write_frame(&mut out, &frame).is_err() {
                    break;
                }
            }
        })?;

    // Reader thread: one line per client frame, forwarded to this loop.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::Builder::new()
        .name("stdin".into())
        .spawn(move || {
            for line in std::io::stdin().lock().lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        })?;

    loop {
        session.pump_events();
        match rx.recv_timeout(PUMP_INTERVAL) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    session.handle_line(&line);
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    let _ = writer.join();
    Ok(())
}

fn write_frame(out: &mut impl Write, frame: &Frame) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(frame).map_err(std::io::Error::other)?;
    line.push(b'\n');
    out.write_all(&line)?;
    out.flush()
}
