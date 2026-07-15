use std::collections::HashMap;
use std::io::{self, BufRead, Write};

use fips_tcp::{ConnectionId, MarkerStatus, SendMarker, Stack, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Command {
    Listen {
        port: u16,
    },
    Connect {
        peer: String,
        local_port: u16,
        remote_port: u16,
        isn: u32,
        now: u64,
    },
    Input {
        peer: String,
        bytes: String,
        now: u64,
    },
    Poll {
        now: u64,
    },
    Accept {
        port: u16,
    },
    Write {
        id: u64,
        bytes: String,
        now: u64,
    },
    WriteWithMarker {
        id: u64,
        bytes: String,
        now: u64,
    },
    MarkerStatus {
        marker: u64,
    },
    Read {
        id: u64,
        max: usize,
        now: u64,
    },
    Close {
        id: u64,
        now: u64,
    },
    State {
        id: u64,
    },
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut stack = Stack::new(fips_tcp::Config::default(), 0x55aa_1234_9988_7766);
    let mut markers = HashMap::new();
    let mut next_marker = 1_u64;

    for line in stdin.lock().lines() {
        let response = match line {
            Ok(line) => serde_json::from_str::<Command>(&line)
                .map_err(|error| error.to_string())
                .and_then(|command| execute(&mut stack, &mut markers, &mut next_marker, command)),
            Err(error) => Err(error.to_string()),
        };
        let output = match response {
            Ok(result) => json!({
                "ok": true,
                "result": result,
                "outbound": drain_outbound(&mut stack),
            }),
            Err(error) => json!({
                "ok": false,
                "error": error,
                "outbound": drain_outbound(&mut stack),
            }),
        };
        serde_json::to_writer(&mut stdout, &output).expect("serialize driver response");
        writeln!(&mut stdout).expect("write driver response");
        stdout.flush().expect("flush driver response");
    }
}

fn execute(
    stack: &mut Stack<String>,
    markers: &mut HashMap<u64, SendMarker>,
    next_marker: &mut u64,
    command: Command,
) -> Result<Value, String> {
    match command {
        Command::Listen { port } => stack.listen(port).map(|()| Value::Null),
        Command::Connect {
            peer,
            local_port,
            remote_port,
            isn,
            now,
        } => stack
            .connect_from_with_isn(peer, local_port, remote_port, isn, now)
            .map(|id| json!(id.get())),
        Command::Input { peer, bytes, now } => stack
            .input(peer, &decode_hex(&bytes)?, now)
            .map(|()| Value::Null),
        Command::Poll { now } => {
            stack.poll(now);
            Ok(Value::Null)
        }
        Command::Accept { port } => Ok(stack.accept(port).map(ConnectionId::get).into()),
        Command::Write { id, bytes, now } => stack
            .write(connection_id(id), &decode_hex(&bytes)?, now)
            .map(|accepted| json!(accepted)),
        Command::WriteWithMarker { id, bytes, now } => stack
            .write_with_marker(connection_id(id), &decode_hex(&bytes)?, now)
            .map(|(accepted, marker)| {
                let handle = *next_marker;
                *next_marker = next_marker
                    .checked_add(1)
                    .expect("interop marker handle space exhausted");
                markers.insert(handle, marker);
                json!({ "accepted": accepted, "marker": handle })
            }),
        Command::MarkerStatus { marker } => {
            let Some(marker) = markers.get(&marker) else {
                return Err("unknown marker handle".to_string());
            };
            return Ok(match stack.marker_status(marker) {
                MarkerStatus::Pending => json!("pending"),
                MarkerStatus::Acked => json!("acked"),
                MarkerStatus::ConnectionGone => json!("connection-gone"),
            });
        }
        Command::Read { id, max, now } => stack
            .read(connection_id(id), max, now)
            .map(|bytes| json!(encode_hex(&bytes))),
        Command::Close { id, now } => stack.close(connection_id(id), now).map(|()| Value::Null),
        Command::State { id } => Ok(stack
            .state(connection_id(id))
            .map(state_name)
            .map(Value::from)
            .unwrap_or(Value::Null)),
    }
    .map_err(|error| error.to_string())
}

fn connection_id(value: u64) -> ConnectionId {
    ConnectionId::from_raw(value)
}

fn drain_outbound(stack: &mut Stack<String>) -> Vec<Value> {
    stack
        .drain_outbound()
        .into_iter()
        .map(|outbound| json!({ "peer": outbound.peer, "bytes": encode_hex(&outbound.bytes) }))
        .collect()
}

fn state_name(state: State) -> &'static str {
    match state {
        State::SynSent => "syn-sent",
        State::SynReceived => "syn-received",
        State::Established => "established",
        State::FinWait1 => "fin-wait-1",
        State::FinWait2 => "fin-wait-2",
        State::CloseWait => "close-wait",
        State::Closing => "closing",
        State::LastAck => "last-ack",
        State::TimeWait => "time-wait",
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex string has odd length".to_string());
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("write to string");
    }
    output
}
