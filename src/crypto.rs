use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;

lazy_static! {
    static ref ENCRYPTION_KEY: Vec<u8> = {
        let key_hex = std::env::var("WORKA_SECRET_ENCRYPTION_KEY")
            .expect("WORKA_SECRET_ENCRYPTION_KEY must be set to a 64-character hex string");
        hex::decode(key_hex).expect("WORKA_SECRET_ENCRYPTION_KEY must be a valid hex string")
    };
}

// Encrypts data using AES-256-GCM.
// The nonce is prepended to the ciphertext.
pub fn encrypt(plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(&ENCRYPTION_KEY).map_err(|e| anyhow!(e.to_string()))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message

    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| anyhow!(e.to_string()))?;

    let mut result = Vec::with_capacity(nonce.len() + ciphertext.len());
    result.extend_from_slice(nonce.as_slice());
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

// Decrypts data using AES-256-GCM.
// Expects the nonce to be prepended to the ciphertext.
pub fn decrypt(encrypted_data: &[u8]) -> Result<Vec<u8>> {
    if encrypted_data.len() < 12 {
        return Err(anyhow!("Invalid encrypted data length"));
    }
    let (nonce_bytes, ciphertext) = encrypted_data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(&ENCRYPTION_KEY).map_err(|e| anyhow!(e.to_string()))?;

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow!(e.to_string()))?;

    Ok(plaintext)
}
