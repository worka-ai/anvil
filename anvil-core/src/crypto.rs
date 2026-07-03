use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, anyhow};
use std::collections::BTreeMap;

const ENVELOPE_MAGIC: &[u8; 8] = b"ANVILK01";
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct EncryptionKeyring {
    active_key_id: String,
    keys: BTreeMap<String, Vec<u8>>,
}

impl EncryptionKeyring {
    pub fn new(active_key_id: impl Into<String>, active_key: Vec<u8>) -> Result<Self> {
        let active_key_id = validate_key_id(active_key_id.into())?;
        validate_key_len(&active_key)?;
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), active_key);
        Ok(Self {
            active_key_id,
            keys,
        })
    }

    pub fn from_hex_config(
        active_key_id: &str,
        active_key_hex: &str,
        previous_keys: &str,
    ) -> Result<Self> {
        let active_key = decode_key_hex(active_key_hex)?;
        let mut keyring = Self::new(active_key_id, active_key)?;
        for item in previous_keys
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let (key_id, key_hex) = item
                .split_once(':')
                .ok_or_else(|| anyhow!("previous encryption key entries must be key_id:hex"))?;
            keyring.insert_previous_key(key_id, decode_key_hex(key_hex)?)?;
        }
        Ok(keyring)
    }

    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    pub fn insert_previous_key(&mut self, key_id: &str, key: Vec<u8>) -> Result<()> {
        let key_id = validate_key_id(key_id.to_string())?;
        validate_key_len(&key)?;
        if key_id == self.active_key_id {
            anyhow::bail!("previous key id must not equal active key id");
        }
        if self.keys.insert(key_id.clone(), key).is_some() {
            anyhow::bail!("duplicate encryption key id '{key_id}'");
        }
        Ok(())
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = self
            .keys
            .get(&self.active_key_id)
            .ok_or_else(|| anyhow!("active encryption key is not present in keyring"))?;
        encrypt_with_key_id(plaintext, &self.active_key_id, key)
    }

    pub fn decrypt(&self, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        let key_id = envelope_key_id(encrypted_data)?;
        let key = self
            .keys
            .get(key_id)
            .ok_or_else(|| anyhow!("encryption key id '{key_id}' is not configured"))?;
        decrypt_with_key(encrypted_data, key)
    }

    pub fn encrypted_with_active_key(&self, encrypted_data: &[u8]) -> Result<bool> {
        Ok(envelope_key_id(encrypted_data)? == self.active_key_id)
    }

    pub fn reencrypt_if_needed(&self, encrypted_data: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.encrypted_with_active_key(encrypted_data)? {
            return Ok(None);
        }
        let plaintext = self.decrypt(encrypted_data)?;
        Ok(Some(self.encrypt(&plaintext)?))
    }
}

pub fn generate_key_hex() -> String {
    let key = Aes256Gcm::generate_key(&mut OsRng);
    hex::encode(key)
}

pub fn decode_key_hex(key_hex: &str) -> Result<Vec<u8>> {
    let key = hex::decode(key_hex.trim()).context("encryption key must be hex encoded")?;
    validate_key_len(&key)?;
    Ok(key)
}

/// Encrypt with a versioned Anvil envelope.
///
/// This intentionally does not emit the pre-envelope nonce+ciphertext format.
/// Fresh clusters must use envelope records only.
pub fn encrypt(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    encrypt_with_key_id(plaintext, "primary", key)
}

/// Decrypt a versioned Anvil envelope.
///
/// This intentionally rejects the pre-envelope nonce+ciphertext format.
pub fn decrypt(encrypted_data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    decrypt_with_key(encrypted_data, key)
}

fn encrypt_with_key_id(plaintext: &[u8], key_id: &str, key: &[u8]) -> Result<Vec<u8>> {
    validate_key_len(key)?;
    let key_id = validate_key_id(key_id.to_string())?;
    let key_id_bytes = key_id.as_bytes();
    let key_id_len =
        u16::try_from(key_id_bytes.len()).map_err(|_| anyhow!("encryption key id is too long"))?;

    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| anyhow!(e.to_string()))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| anyhow!(e.to_string()))?;

    let mut result = Vec::with_capacity(
        ENVELOPE_MAGIC.len() + 2 + NONCE_LEN + key_id_bytes.len() + ciphertext.len(),
    );
    result.extend_from_slice(ENVELOPE_MAGIC);
    result.extend_from_slice(&key_id_len.to_be_bytes());
    #[allow(deprecated)]
    result.extend_from_slice(nonce.as_slice());
    result.extend_from_slice(key_id_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

fn decrypt_with_key(encrypted_data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    validate_key_len(key)?;
    let parsed = parse_envelope(encrypted_data)?;
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| anyhow!(e.to_string()))?;
    #[allow(deprecated)]
    let nonce = Nonce::from_slice(parsed.nonce);
    cipher
        .decrypt(nonce, parsed.ciphertext)
        .map_err(|e| anyhow!(e.to_string()))
}

fn envelope_key_id(encrypted_data: &[u8]) -> Result<&str> {
    parse_envelope(encrypted_data).map(|parsed| parsed.key_id)
}

struct ParsedEnvelope<'a> {
    nonce: &'a [u8],
    key_id: &'a str,
    ciphertext: &'a [u8],
}

fn parse_envelope(encrypted_data: &[u8]) -> Result<ParsedEnvelope<'_>> {
    let minimum = ENVELOPE_MAGIC.len() + 2 + NONCE_LEN + 1;
    if encrypted_data.len() < minimum {
        anyhow::bail!("encrypted data is not an Anvil key envelope");
    }
    if &encrypted_data[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC {
        anyhow::bail!("encrypted data is not an Anvil key envelope");
    }
    let key_id_len_offset = ENVELOPE_MAGIC.len();
    let key_id_len = u16::from_be_bytes([
        encrypted_data[key_id_len_offset],
        encrypted_data[key_id_len_offset + 1],
    ]) as usize;
    if key_id_len == 0 {
        anyhow::bail!("encrypted envelope has an empty key id");
    }
    let nonce_offset = ENVELOPE_MAGIC.len() + 2;
    let key_id_offset = nonce_offset + NONCE_LEN;
    let ciphertext_offset = key_id_offset
        .checked_add(key_id_len)
        .ok_or_else(|| anyhow!("encrypted envelope key id length overflow"))?;
    if encrypted_data.len() <= ciphertext_offset {
        anyhow::bail!("encrypted envelope has no ciphertext");
    }
    let key_id = std::str::from_utf8(&encrypted_data[key_id_offset..ciphertext_offset])
        .context("encrypted envelope key id must be utf-8")?;
    validate_key_id(key_id.to_string())?;
    Ok(ParsedEnvelope {
        nonce: &encrypted_data[nonce_offset..key_id_offset],
        key_id,
        ciphertext: &encrypted_data[ciphertext_offset..],
    })
}

fn validate_key_id(key_id: String) -> Result<String> {
    if key_id.is_empty()
        || key_id.len() > 128
        || key_id.contains(':')
        || key_id.contains(',')
        || key_id.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        anyhow::bail!("encryption key id must be 1-128 visible chars excluding ':' and ','");
    }
    Ok(key_id)
}

fn validate_key_len(key: &[u8]) -> Result<()> {
    if key.len() != KEY_LEN {
        anyhow::bail!("encryption key must be exactly 32 bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_records_key_id() {
        let keyring = EncryptionKeyring::from_hex_config(
            "k2",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "k1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();

        let encrypted = keyring.encrypt(b"secret").unwrap();

        assert_eq!(envelope_key_id(&encrypted).unwrap(), "k2");
        assert_eq!(keyring.decrypt(&encrypted).unwrap(), b"secret");
        assert!(keyring.encrypted_with_active_key(&encrypted).unwrap());
    }

    #[test]
    fn previous_key_records_are_reencrypted_to_active_key() {
        let previous = EncryptionKeyring::from_hex_config(
            "old",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "",
        )
        .unwrap();
        let encrypted = previous.encrypt(b"secret").unwrap();

        let rotated = EncryptionKeyring::from_hex_config(
            "new",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "old:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();

        let reencrypted = rotated.reencrypt_if_needed(&encrypted).unwrap().unwrap();

        assert_eq!(rotated.decrypt(&reencrypted).unwrap(), b"secret");
        assert_eq!(envelope_key_id(&reencrypted).unwrap(), "new");
    }

    #[test]
    fn raw_nonce_ciphertext_is_rejected() {
        let raw = vec![0_u8; 48];
        let key = [0_u8; 32];

        assert!(decrypt(&raw, &key).is_err());
    }
}
