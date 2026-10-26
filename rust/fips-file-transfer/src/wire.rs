use crate::{CHUNK, ManifestFile, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub const MAX_CONTROL: usize = 16 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Control {
    Accept {
        id: String,
        token: String,
    },
    Decline {
        id: String,
        token: String,
    },
    Offer {
        id: String,
        token: String,
        version: u8,
        files: Vec<ManifestFile>,
    },
    Offered {
        id: String,
    },
    Resume {
        id: String,
        token: String,
        manifest: String,
        offset: u64,
        prefix_sha256: String,
    },
    Ready {
        id: String,
        offset: u64,
    },
    Rejected {
        id: String,
        reason: Rejection,
    },
    Done,
    Received,
    Cancel,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Rejection {
    Unavailable,
    Mismatch,
    Expired,
}
pub fn valid_filename(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 240
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c.is_control() || "/\\<>:\"|?*".contains(c))
}
pub fn valid_transfer_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
pub fn validate_claim(id: &str, token: &str) -> Result<()> {
    if !valid_transfer_id(id) || token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("Invalid file offer".into());
    }
    Ok(())
}
pub fn validate_manifest(files: &[ManifestFile]) -> Result<()> {
    if files.is_empty() || files.len() > 32 {
        return Err("Choose between 1 and 32 files".into());
    }
    for f in files {
        if !valid_filename(&f.filename)
            || f.size_bytes > 100 * 1024 * 1024 * 1024
            || f.sha256.len() != 64
            || !f.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("Invalid file details".into());
        }
    }
    Ok(())
}
/// Canonical manifest digest shared with TypeScript: count:u32BE, then each
/// UTF-8 name length:u32BE/name, size:u64BE and raw SHA-256 bytes.
pub fn manifest_hash(files: &[ManifestFile]) -> Result<String> {
    validate_manifest(files)?;
    let mut hash = Sha256::new();
    hash.update((files.len() as u32).to_be_bytes());
    for f in files {
        hash.update((f.filename.len() as u32).to_be_bytes());
        hash.update(f.filename.as_bytes());
        hash.update(f.size_bytes.to_be_bytes());
        for pair in f.sha256.as_bytes().chunks_exact(2) {
            let text = std::str::from_utf8(pair).map_err(|_| "Invalid file hash")?;
            hash.update([u8::from_str_radix(text, 16).map_err(|_| "Invalid file hash")?]);
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn frame(kind: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 5);
    out.extend_from_slice(&((bytes.len() + 1) as u32).to_be_bytes());
    out.push(kind);
    out.extend_from_slice(bytes);
    out
}
pub fn control(value: &Control) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() + 1 > MAX_CONTROL {
        return Err("File offer is too large".into());
    }
    Ok(frame(0, &bytes))
}
pub fn data(bytes: &[u8]) -> Vec<u8> {
    frame(1, bytes)
}
pub fn parse_control(packet: &[u8]) -> Result<Control> {
    if packet.first() != Some(&0) || packet.len() > MAX_CONTROL {
        return Err("Invalid file control".into());
    }
    serde_json::from_slice(&packet[1..]).map_err(|_| "Invalid file control".into())
}
#[derive(Default)]
pub struct Reader {
    bytes: Vec<u8>,
}
impl Reader {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
        if self.bytes.len() + bytes.len() > 2 * (CHUNK + 5) {
            return Err("File receive buffer exceeded".into());
        }
        self.bytes.extend_from_slice(bytes);
        let mut consumed = 0;
        let mut packets = Vec::new();
        while self.bytes.len() - consumed >= 4 {
            let length = u32::from_be_bytes(
                self.bytes[consumed..consumed + 4]
                    .try_into()
                    .map_err(|_| "Invalid file header")?,
            ) as usize;
            if length == 0 || length > CHUNK + 1 {
                return Err("Invalid file frame size".into());
            }
            let end = consumed + 4 + length;
            if end > self.bytes.len() {
                break;
            }
            packets.push(self.bytes[consumed + 4..end].to_vec());
            consumed = end;
        }
        self.bytes.drain(..consumed);
        Ok(packets)
    }
}
