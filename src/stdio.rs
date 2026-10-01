//! The SSH-exec (`stdio`) transport: newline-delimited JSON on stdin/stdout.
//!
//! The mobile app runs `herdr-mobile-bridge stdio --session <name>` over SSH;
//! this module is the loop that keeps a [`Session`] fed and drained.

use crate::proto::Frame;
use crate::session::{Session, PUMP_INTERVAL};
use std::io::{BufRead, Write};
use std::sync::mpsc::{self, RecvTimeoutError};

/// Run a stdio bridge session until the app closes stdin.
pub fn run(socket_path: &str, session_name: &str) -> std::io::Result<()> {
    let mut session = Session::connect(socket_path, session_name)?;

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

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    loop {
        for frame in session.pump_events() {
            write_frame(&mut out, &frame)?;
        }
        match rx.recv_timeout(PUMP_INTERVAL) {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                for frame in session.handle_line(&line) {
                    write_frame(&mut out, &frame)?;
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn write_frame(out: &mut impl Write, frame: &Frame) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(frame).map_err(std::io::Error::other)?;
    line.push(b'\n');
    out.write_all(&line)?;
    out.flush()
}
