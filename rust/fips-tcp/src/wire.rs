use std::fmt;

pub const HEADER_LEN: usize = 20;
pub const FIPS_OPTION_KIND: u8 = 254;
pub const FIPS_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Flags(u16);

impl Flags {
    pub const FIN: Self = Self(0x001);
    pub const SYN: Self = Self(0x002);
    pub const RST: Self = Self(0x004);
    pub const PSH: Self = Self(0x008);
    pub const ACK: Self = Self(0x010);
    pub const ECE: Self = Self(0x040);
    pub const CWR: Self = Self(0x080);

    const SUPPORTED_MASK: u16 = Self::FIN.0
        | Self::SYN.0
        | Self::RST.0
        | Self::PSH.0
        | Self::ACK.0
        | Self::ECE.0
        | Self::CWR.0;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    pub const fn union(self, flag: Self) -> Self {
        Self(self.0 | flag.0)
    }

    fn from_wire(bits: u16) -> Result<Self, WireError> {
        if bits & !Self::SUPPORTED_MASK != 0 {
            return Err(WireError::UnsupportedFlags(bits));
        }
        Ok(Self(bits))
    }
}

impl std::ops::BitOr for Flags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpOption {
    EndOfList,
    NoOperation,
    MaxSegmentSize(u16),
    FipsVersion { version: u8, reserved: u8 },
    Unknown { kind: u8, data: Vec<u8> },
}

impl TcpOption {
    fn encoded_len(&self) -> usize {
        match self {
            Self::EndOfList | Self::NoOperation => 1,
            Self::MaxSegmentSize(_) | Self::FipsVersion { .. } => 4,
            Self::Unknown { data, .. } => data.len() + 2,
        }
    }

    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::EndOfList => output.push(0),
            Self::NoOperation => output.push(1),
            Self::MaxSegmentSize(value) => {
                output.extend_from_slice(&[2, 4]);
                output.extend_from_slice(&value.to_be_bytes());
            }
            Self::FipsVersion { version, reserved } => {
                output.extend_from_slice(&[FIPS_OPTION_KIND, 4, *version, *reserved]);
            }
            Self::Unknown { kind, data } => {
                let len = data.len() + 2;
                let len = u8::try_from(len).map_err(|_| WireError::OptionTooLong)?;
                if *kind <= 1 {
                    return Err(WireError::MalformedOption);
                }
                output.extend_from_slice(&[*kind, len]);
                output.extend_from_slice(data);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub src_port: u16,
    pub dst_port: u16,
    pub seq: u32,
    pub ack: Option<u32>,
    pub flags: Flags,
    pub window: u16,
    pub options: Vec<TcpOption>,
    pub payload: Vec<u8>,
}

impl Segment {
    pub fn new(src_port: u16, dst_port: u16, seq: u32) -> Self {
        Self {
            src_port,
            dst_port,
            seq,
            ack: None,
            flags: Flags::empty(),
            window: u16::MAX,
            options: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn fips_version(&self) -> Option<u8> {
        self.options.iter().find_map(|option| match option {
            TcpOption::FipsVersion { version, .. } => Some(*version),
            _ => None,
        })
    }

    pub fn supports_fips_version(&self, expected: u8) -> bool {
        self.options.iter().any(|option| {
            matches!(
                option,
                TcpOption::FipsVersion {
                    version,
                    reserved: 0
                } if *version == expected
            )
        })
    }

    pub fn max_segment_size(&self) -> Option<u16> {
        self.options.iter().find_map(|option| match option {
            TcpOption::MaxSegmentSize(value) => Some(*value),
            _ => None,
        })
    }

    pub fn sequence_len(&self) -> u32 {
        self.payload.len() as u32
            + u32::from(self.flags.contains(Flags::SYN))
            + u32::from(self.flags.contains(Flags::FIN))
    }

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        if self.src_port == 0 || self.dst_port == 0 {
            return Err(WireError::ZeroPort);
        }
        if self.flags.contains(Flags::ACK) != self.ack.is_some() {
            return Err(WireError::AckFlagMismatch);
        }

        let options_len: usize = self.options.iter().map(TcpOption::encoded_len).sum();
        let padded_options_len = options_len.next_multiple_of(4);
        let header_len = HEADER_LEN + padded_options_len;
        if header_len > 60 {
            return Err(WireError::HeaderTooLong);
        }

        let mut output = Vec::with_capacity(header_len + self.payload.len());
        output.extend_from_slice(&self.src_port.to_be_bytes());
        output.extend_from_slice(&self.dst_port.to_be_bytes());
        output.extend_from_slice(&self.seq.to_be_bytes());
        output.extend_from_slice(&self.ack.unwrap_or(0).to_be_bytes());
        let offset_and_flags = ((header_len as u16 / 4) << 12) | self.flags.bits();
        output.extend_from_slice(&offset_and_flags.to_be_bytes());
        output.extend_from_slice(&self.window.to_be_bytes());
        output.extend_from_slice(&0u16.to_be_bytes());
        output.extend_from_slice(&0u16.to_be_bytes());
        for option in &self.options {
            option.encode_into(&mut output)?;
        }
        output.resize(header_len, 0);
        output.extend_from_slice(&self.payload);
        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < HEADER_LEN {
            return Err(WireError::Truncated);
        }
        let src_port = u16::from_be_bytes([bytes[0], bytes[1]]);
        let dst_port = u16::from_be_bytes([bytes[2], bytes[3]]);
        if src_port == 0 || dst_port == 0 {
            return Err(WireError::ZeroPort);
        }
        let seq = u32::from_be_bytes(bytes[4..8].try_into().expect("fixed field"));
        let raw_ack = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed field"));
        let offset_and_flags = u16::from_be_bytes(bytes[12..14].try_into().expect("fixed field"));
        let header_len = usize::from(offset_and_flags >> 12) * 4;
        if !(HEADER_LEN..=60).contains(&header_len) || header_len > bytes.len() {
            return Err(WireError::InvalidHeaderLength);
        }
        if u16::from_be_bytes([bytes[16], bytes[17]]) != 0 {
            return Err(WireError::NonZeroChecksum);
        }
        if u16::from_be_bytes([bytes[18], bytes[19]]) != 0 {
            return Err(WireError::UrgentUnsupported);
        }
        let flags = Flags::from_wire(offset_and_flags & 0x1ff)?;
        let ack = flags.contains(Flags::ACK).then_some(raw_ack);
        let window = u16::from_be_bytes([bytes[14], bytes[15]]);
        let options = decode_options(&bytes[HEADER_LEN..header_len])?;
        Ok(Self {
            src_port,
            dst_port,
            seq,
            ack,
            flags,
            window,
            options,
            payload: bytes[header_len..].to_vec(),
        })
    }
}

fn decode_options(mut bytes: &[u8]) -> Result<Vec<TcpOption>, WireError> {
    let mut options = Vec::new();
    while let Some((&kind, rest)) = bytes.split_first() {
        match kind {
            0 => break,
            1 => {
                options.push(TcpOption::NoOperation);
                bytes = rest;
            }
            _ => {
                let Some(&len) = rest.first() else {
                    return Err(WireError::MalformedOption);
                };
                let len = usize::from(len);
                if len < 2 || len > bytes.len() {
                    return Err(WireError::MalformedOption);
                }
                let data = &bytes[2..len];
                let option = match (kind, len) {
                    (2, 4) => TcpOption::MaxSegmentSize(u16::from_be_bytes([data[0], data[1]])),
                    (FIPS_OPTION_KIND, 4) => TcpOption::FipsVersion {
                        version: data[0],
                        reserved: data[1],
                    },
                    (2 | FIPS_OPTION_KIND, _) => return Err(WireError::MalformedOption),
                    _ => TcpOption::Unknown {
                        kind,
                        data: data.to_vec(),
                    },
                };
                options.push(option);
                bytes = &bytes[len..];
            }
        }
    }
    Ok(options)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Truncated,
    InvalidHeaderLength,
    NonZeroChecksum,
    UrgentUnsupported,
    UnsupportedFlags(u16),
    ZeroPort,
    AckFlagMismatch,
    HeaderTooLong,
    OptionTooLong,
    MalformedOption,
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated TCP/FIPS segment"),
            Self::InvalidHeaderLength => formatter.write_str("invalid TCP/FIPS header length"),
            Self::NonZeroChecksum => formatter.write_str("TCP/FIPS checksum must be zero"),
            Self::UrgentUnsupported => formatter.write_str("TCP/FIPS urgent data is unsupported"),
            Self::UnsupportedFlags(bits) => write!(formatter, "unsupported TCP flags: {bits:#x}"),
            Self::ZeroPort => formatter.write_str("TCP/FIPS ports must be non-zero"),
            Self::AckFlagMismatch => formatter.write_str("ACK flag and ACK number disagree"),
            Self::HeaderTooLong => formatter.write_str("TCP/FIPS header exceeds 60 bytes"),
            Self::OptionTooLong => formatter.write_str("TCP option exceeds 255 bytes"),
            Self::MalformedOption => formatter.write_str("malformed TCP option"),
        }
    }
}

impl std::error::Error for WireError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_length_counts_syn_and_fin() {
        let mut segment = Segment::new(1, 2, u32::MAX);
        segment.flags = Flags::SYN | Flags::FIN;
        segment.payload = vec![1, 2, 3];
        assert_eq!(segment.sequence_len(), 5);
    }
}
