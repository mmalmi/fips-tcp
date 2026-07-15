use fips_tcp::wire::{Flags, Segment, TcpOption};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    name: String,
    hex: String,
    src_port: u16,
    dst_port: u16,
    seq: u32,
    ack: Option<u32>,
    flags: Vec<String>,
    window: u16,
    mss: Option<u16>,
    version: Option<u8>,
    payload_hex: String,
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2));
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn shared_wire_vectors_decode_and_reencode_exactly() {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/wire-vectors.json")).unwrap();

    for vector in vectors {
        let bytes = decode_hex(&vector.hex);
        let segment = Segment::decode(&bytes)
            .unwrap_or_else(|error| panic!("{} should decode: {error}", vector.name));
        assert_eq!(segment.src_port, vector.src_port, "{}", vector.name);
        assert_eq!(segment.dst_port, vector.dst_port, "{}", vector.name);
        assert_eq!(segment.seq, vector.seq, "{}", vector.name);
        assert_eq!(segment.ack, vector.ack, "{}", vector.name);
        assert_eq!(segment.window, vector.window, "{}", vector.name);
        assert_eq!(
            segment.payload,
            decode_hex(&vector.payload_hex),
            "{}",
            vector.name
        );
        assert_eq!(
            segment.flags.contains(Flags::SYN),
            vector.flags.iter().any(|v| v == "syn")
        );
        assert_eq!(
            segment.flags.contains(Flags::ACK),
            vector.flags.iter().any(|v| v == "ack")
        );
        assert_eq!(
            segment.options.iter().find_map(|option| match option {
                TcpOption::MaxSegmentSize(value) => Some(*value),
                _ => None,
            }),
            vector.mss,
            "{}",
            vector.name
        );
        assert_eq!(segment.fips_version(), vector.version, "{}", vector.name);
        assert_eq!(segment.encode().unwrap(), bytes, "{}", vector.name);
    }
}

#[test]
fn malformed_segments_are_rejected() {
    assert!(Segment::decode(&[0; 19]).is_err());
    let mut nonzero_checksum = decode_hex("01bbc0005566778811223345501080000001000068656c6c6f");
    assert!(Segment::decode(&nonzero_checksum).is_err());
    nonzero_checksum[16] = 0;
    nonzero_checksum[17] = 0;
    assert!(Segment::decode(&nonzero_checksum).is_ok());
}
