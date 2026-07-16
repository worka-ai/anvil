use anyhow::{Context, Result, bail};
use prost::Message;
use sha2::{Digest, Sha256};

/// Encode a fixed-schema internal CoreStore protobuf message.
///
/// Internal CoreStore control records use protobuf as a schema language, but the
/// stored bytes must still be canonical: decoding and re-encoding must produce
/// byte-for-byte identical output. That rejects unknown fields and alternative
/// encodings before the record is accepted as durable CoreStore metadata.
pub fn encode_deterministic_proto<M>(message: &M) -> Vec<u8>
where
    M: Message,
{
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut bytes)
        .expect("encoding a protobuf message into Vec cannot fail");
    bytes
}

pub fn decode_deterministic_proto<M>(bytes: &[u8], label: &'static str) -> Result<M>
where
    M: Message + Default,
{
    let decoded = M::decode(bytes).with_context(|| format!("decode {label}"))?;
    let canonical = encode_deterministic_proto(&decoded);
    if canonical != bytes {
        bail!("{label} is not canonical deterministic protobuf");
    }
    Ok(decoded)
}

pub fn protobuf_sha256_hex<M>(message: &M) -> String
where
    M: Message,
{
    sha256_hex(&encode_deterministic_proto(message))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}
