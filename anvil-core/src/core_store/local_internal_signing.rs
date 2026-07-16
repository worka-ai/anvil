use super::*;

impl CoreStore {
    pub fn register_node_receipt_signing_public_key(
        &self,
        node_id: &str,
        public_key_protobuf: &[u8],
    ) -> Result<String> {
        store_node_receipt_signing_public_key(&self.meta, node_id, public_key_protobuf)
    }

    pub fn sign_internal_core_receipt(&self, signed_payload_hash: &str) -> Result<Vec<u8>> {
        self.sign_core_receipt(signed_payload_hash)
    }

    pub fn verify_internal_core_receipt_signature(
        &self,
        node_id: &str,
        signed_payload_hash: &str,
        receipt_signature: &[u8],
    ) -> Result<()> {
        if node_id.trim().is_empty() {
            bail!("CoreStore internal receipt node id must not be empty");
        }
        let public_key = if node_id == self.node_identity.node_id
            || is_local_shard_node_id(node_id)
            || is_local_control_node_id(node_id)
        {
            self.node_signing_keypair.public()
        } else {
            load_node_receipt_signing_public_key(&self.meta, node_id)?.ok_or_else(|| {
                anyhow!("CoreStore internal receipt references unknown node {node_id}")
            })?
        };
        if !public_key.verify(signed_payload_hash.as_bytes(), receipt_signature) {
            bail!("CoreStore internal receipt signature verification failed for node {node_id}");
        }
        Ok(())
    }
}
