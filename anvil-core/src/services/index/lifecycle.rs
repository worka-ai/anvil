use super::*;

impl AppState {
    pub(super) async fn get_index_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))
    }

    pub(super) async fn publish_index_definition_event(
        &self,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        event_type: &str,
    ) -> Result<crate::persistence::IndexDefinitionEvent, Status> {
        self.publish_index_definition_event_with_transaction(bucket, index, event_type, None, None)
            .await
    }

    pub(super) async fn publish_index_definition_event_with_transaction(
        &self,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        event_type: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<crate::persistence::IndexDefinitionEvent, Status> {
        let event = self
            .persistence
            .create_index_definition_event_with_transaction(
                bucket.tenant_id,
                bucket.id,
                &bucket.name,
                index,
                event_type,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(event)
    }
}
