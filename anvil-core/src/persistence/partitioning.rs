use super::*;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::LazyLock,
};
use tokio::sync::Mutex as TokioMutex;

const PARTITION_OWNER_ACQUIRE_LOCK_STRIPES: usize = 256;
const PARTITION_OWNER_ACQUIRE_ATTEMPTS: usize = 32;

static PARTITION_OWNER_ACQUIRE_LOCKS: LazyLock<Vec<TokioMutex<()>>> = LazyLock::new(|| {
    (0..PARTITION_OWNER_ACQUIRE_LOCK_STRIPES)
        .map(|_| TokioMutex::new(()))
        .collect()
});

fn partition_owner_acquire_lock(
    partition_family: &str,
    partition_id: &str,
) -> &'static TokioMutex<()> {
    let mut hasher = DefaultHasher::new();
    partition_family.hash(&mut hasher);
    partition_id.hash(&mut hasher);
    let stripe = (hasher.finish() as usize) % PARTITION_OWNER_ACQUIRE_LOCK_STRIPES;
    &PARTITION_OWNER_ACQUIRE_LOCKS[stripe]
}

fn is_partition_owner_cas_conflict(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .to_string()
            .contains("CoreStore partition owner CAS conflict")
    })
}

impl Persistence {
    pub fn new(config: &Config, event_publisher: Option<Sender<MetadataEvent>>) -> Result<Self> {
        Ok(Self {
            storage: Storage::new_at_sync(&config.storage_path)?,
            cache: MetadataCache::new(config),
            core_store: Arc::new(OnceCell::new()),
            event_publisher,
            task_notify: Arc::new(Notify::new()),
            mesh_id: nonempty_or(&config.mesh_id, "default"),
            region: nonempty_or(&config.region, "default"),
            cell_id: nonempty_or(&config.cell_id, "default"),
            owner_node_id: persistence_owner_node_id(config),
            partition_owner_signing_key: hex::decode(&config.anvil_secret_encryption_key)?,
            personaldb_signing_key: config.anvil_secret_encryption_key.as_bytes().to_vec(),
            embedding_providers: EmbeddingProviderRegistry::from_config(config)?,
            object_metadata_compaction_frame_threshold: config
                .object_metadata_compaction_frame_threshold,
            object_metadata_compaction_bytes_threshold: config
                .object_metadata_compaction_bytes_threshold,
            task_lease_ttl_secs: if config.task_lease_ttl_secs == 0 {
                300
            } else {
                config.task_lease_ttl_secs
            },
        })
    }

    pub(super) async fn core_store(&self) -> Result<CoreStore> {
        self.core_store
            .get_or_try_init(|| async { CoreStore::new(self.storage.clone()).await })
            .await
            .cloned()
    }

    pub(super) async fn publish_event(&self, event: MetadataEvent) {
        if let Some(sender) = &self.event_publisher {
            let _ = sender.send(event).await;
        }
    }

    pub fn task_notify(&self) -> Arc<Notify> {
        self.task_notify.clone()
    }

    pub(crate) fn owner_node_id(&self) -> &str {
        &self.owner_node_id
    }

    pub(super) fn notify_task_enqueued(&self) {
        self.task_notify.notify_waiters();
    }

    pub(super) async fn write_mesh_tenant_locators(
        &self,
        tenant: &Tenant,
        idempotency_key: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let reservation_expires_at = (Utc::now() + Duration::minutes(5)).to_rfc3339();
        let mesh_id = mesh_directory::MeshId::new(self.mesh_id.clone())?;
        let tenant_id = mesh_directory::TenantId::new(tenant.id.to_string())?;
        let tenant_name = mesh_directory::TenantName::canonicalize(&tenant.name)?;
        let home_region = mesh_directory::RegionName::new(self.region.clone())?;
        let reserved_name = mesh_directory::TenantNameDescriptor::reserved(
            mesh_id.clone(),
            tenant_name.clone(),
            tenant_id.clone(),
            idempotency_key,
            reservation_expires_at,
            now.clone(),
        )?;
        let locator_descriptor = mesh_directory::TenantLocatorDescriptor::active(
            mesh_id,
            tenant_id.clone(),
            tenant_name.clone(),
            home_region,
            now.clone(),
        )?;
        let tenant_name_permit = self
            .mesh_control_write_permit(
                mesh_directory::RoutingRecordFamily::TenantName,
                &reserved_name.partition(),
            )
            .await?;
        let tenant_locator_permit = self
            .mesh_control_write_permit(
                mesh_directory::RoutingRecordFamily::TenantLocator,
                &locator_descriptor.partition(),
            )
            .await?;
        let tenant_name_authority = mesh_directory::MeshControlWriteAuthority {
            permit: &tenant_name_permit,
            signing_key: &self.partition_owner_signing_key,
        };
        let tenant_locator_authority = mesh_directory::MeshControlWriteAuthority {
            permit: &tenant_locator_permit,
            signing_key: &self.partition_owner_signing_key,
        };
        let reserved = mesh_directory::reserve_tenant_name(
            &self.storage,
            &reserved_name,
            tenant_name_authority,
        )
        .await?;
        mesh_directory::create_tenant_locator(
            &self.storage,
            &locator_descriptor,
            tenant_locator_authority,
        )
        .await?;
        mesh_directory::activate_tenant_name(
            &self.storage,
            &tenant_name,
            &tenant_id,
            reserved.generation,
            now,
            tenant_name_authority,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn write_mesh_bucket_locator(&self, bucket: &Bucket) -> Result<()> {
        let now = bucket.created_at.to_rfc3339();
        let mesh_id = mesh_directory::MeshId::new(self.mesh_id.clone())?;
        let tenant_id = mesh_directory::TenantId::new(bucket.tenant_id.to_string())?;
        let bucket_name = mesh_directory::BucketName::canonicalize(&bucket.name)?;
        let bucket_id = mesh_directory::BucketId::new(bucket.id.to_string())?;
        let home_region = mesh_directory::RegionName::new(bucket.region.clone())?;
        let home_cell = mesh_directory::CellId::new(self.cell_id.clone())?;
        let object_prefix = format!("objects/{tenant_id}/{bucket_name}/");
        let mut locator = mesh_directory::BucketLocatorDescriptor::active(
            mesh_id,
            tenant_id,
            bucket_name,
            bucket_id,
            home_region,
            home_cell,
            "regional-primary",
            object_prefix,
            now,
        )?;
        if let Some(existing) =
            mesh_directory::read_bucket_locator(&self.storage, &locator.key()).await?
            && existing.status == mesh_directory::BucketLocatorStatus::Deleted
        {
            locator.generation = existing.generation.saturating_add(1);
        }
        let permit = self
            .mesh_control_write_permit(
                mesh_directory::RoutingRecordFamily::BucketLocator,
                &locator.partition(),
            )
            .await?;
        mesh_directory::write_bucket_locator(
            &self.storage,
            &locator,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await?;
        Ok(())
    }

    pub(super) async fn mark_mesh_bucket_locator_deleted(&self, bucket: &Bucket) -> Result<()> {
        let tenant_id = mesh_directory::TenantId::new(bucket.tenant_id.to_string())?;
        let bucket_name = mesh_directory::BucketName::canonicalize(&bucket.name)?;
        let key = mesh_directory::BucketLocatorKey::new(tenant_id, bucket_name);
        let Some(existing) = mesh_directory::read_bucket_locator(&self.storage, &key).await? else {
            return Ok(());
        };
        if existing.status == mesh_directory::BucketLocatorStatus::Deleted {
            return Ok(());
        }

        let mut deleted = existing;
        deleted.status = mesh_directory::BucketLocatorStatus::Deleted;
        deleted.updated_at = Utc::now().to_rfc3339();
        deleted.generation = deleted.generation.saturating_add(1);
        self.write_mesh_bucket_locator_descriptor(&deleted).await
    }

    pub async fn get_mesh_tenant_name_locator(
        &self,
        tenant_name: &str,
    ) -> Result<Option<mesh_directory::TenantNameDescriptor>> {
        let tenant_name = mesh_directory::TenantName::canonicalize(tenant_name)?;
        Ok(mesh_directory::read_tenant_name_descriptor(&self.storage, &tenant_name).await?)
    }

    pub async fn get_mesh_bucket_locator(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<Option<mesh_directory::BucketLocatorDescriptor>> {
        let key = mesh_directory::BucketLocatorKey::new(
            mesh_directory::TenantId::new(tenant_id.to_string())?,
            mesh_directory::BucketName::canonicalize(bucket_name)?,
        );
        Ok(mesh_directory::read_bucket_locator(&self.storage, &key).await?)
    }

    pub async fn list_mesh_routing_records(
        &self,
        family_filter: Option<mesh_directory::RoutingRecordFamily>,
    ) -> Result<Vec<mesh_directory::RoutingRecordDescriptor>> {
        Ok(mesh_directory::list_routing_records(&self.storage, family_filter).await?)
    }

    pub async fn diagnose_mesh_routing_projection(
        &self,
        family_filter: Option<mesh_directory::RoutingRecordFamily>,
    ) -> Result<Vec<mesh_control_stream::ControlProjectionDiagnostic>> {
        let mut by_stream =
            BTreeMap::<(mesh_directory::RoutingRecordFamily, String), Vec<_>>::new();
        for family in family_filter
            .map(|family| vec![family])
            .unwrap_or_else(|| mesh_directory::RoutingRecordFamily::all().to_vec())
        {
            for record in
                mesh_directory::list_projected_routing_records(&self.storage, family).await?
            {
                by_stream
                    .entry((record.family, record.partition.clone()))
                    .or_default()
                    .push(mesh_control_stream::ControlProjectionRecord::new(
                        record.record_key,
                        record.generation,
                        record.payload_json.into_bytes(),
                    ));
            }
            let stream_family = family.stream_family();
            for partition in
                mesh_control_stream::list_control_stream_partitions(&self.storage, stream_family)
                    .await?
            {
                by_stream.entry((family, partition)).or_default();
            }
        }

        let mut diagnostics = Vec::new();
        for ((family, partition), projected_records) in by_stream {
            let stream_family = family.stream_family();
            diagnostics.extend(
                mesh_control_stream::diagnose_control_stream_projection(
                    &self.storage,
                    stream_family,
                    &partition,
                    &projected_records,
                )
                .await?,
            );
        }
        Ok(diagnostics)
    }

    pub async fn repair_mesh_routing_record(
        &self,
        family: mesh_directory::RoutingRecordFamily,
        record_key: &str,
    ) -> Result<mesh_directory::RoutingRecordDescriptor> {
        let partition = mesh_directory::routing_record_partition_for_key(family, record_key)?;
        let stream_family = family.stream_family();
        let record = mesh_control_stream::latest_projected_record_from_control_stream(
            &self.storage,
            stream_family,
            &partition,
            record_key,
        )
        .await?
        .ok_or_else(|| {
            anyhow!("no control stream mutation found for {stream_family}/{partition}/{record_key}")
        })?;
        mesh_directory::rebuild_routing_record_projection_from_payload(
            &self.storage,
            family,
            record_key,
            &record.payload_json,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn apply_region_drain_plan(
        &self,
        region: &str,
        default_disposition: crate::mesh_lifecycle::BucketDrainDisposition,
        overrides: Vec<RegionDrainBucketOverride>,
    ) -> Result<RegionDrainPlanReport> {
        let mut overrides_by_bucket = HashMap::new();
        for override_ in overrides {
            let key = (override_.tenant_id.clone(), override_.bucket_name.clone());
            if overrides_by_bucket.insert(key.clone(), override_).is_some() {
                return Err(anyhow!(
                    "duplicate bucket drain override for tenant {} bucket {}",
                    key.0,
                    key.1
                ));
            }
        }

        let mut locators = self.bucket_locators_in_region(region).await?;
        locators.sort_by(|left, right| {
            left.tenant_id
                .as_str()
                .cmp(right.tenant_id.as_str())
                .then(left.bucket_name.as_str().cmp(right.bucket_name.as_str()))
        });
        let drainable_locator_keys = locators
            .iter()
            .filter(|locator| locator.status != mesh_directory::BucketLocatorStatus::Deleted)
            .map(|locator| {
                (
                    locator.tenant_id.as_str().to_string(),
                    locator.bucket_name.as_str().to_string(),
                )
            })
            .collect::<HashSet<_>>();
        for (tenant_id, bucket_name) in overrides_by_bucket.keys() {
            if !drainable_locator_keys.contains(&(tenant_id.clone(), bucket_name.clone())) {
                return Err(anyhow!(
                    "bucket drain override for tenant {tenant_id} bucket {bucket_name} does not match an active bucket locator in region {region}"
                ));
            }
        }

        let mut decisions = Vec::new();
        for locator in locators {
            if locator.status == mesh_directory::BucketLocatorStatus::Deleted {
                continue;
            }
            let tenant_id = locator.tenant_id.as_str().to_string();
            let bucket_name = locator.bucket_name.as_str().to_string();
            let override_ = overrides_by_bucket.get(&(tenant_id.clone(), bucket_name.clone()));
            let disposition = override_
                .map(|override_| override_.disposition)
                .unwrap_or(default_disposition);
            let reason = override_
                .map(|override_| override_.reason.clone())
                .unwrap_or_else(|| "region drain default disposition".to_string());
            let expires_at = override_.and_then(|override_| override_.expires_at.clone());

            let status_before = locator.status;
            let mut status_after = status_before;
            let mut exception_written = false;
            match disposition {
                crate::mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty => {}
                crate::mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly
                | crate::mesh_lifecycle::BucketDrainDisposition::ReadOnlyUntilRemoved => {
                    status_after = mesh_directory::BucketLocatorStatus::ReadOnly;
                    crate::mesh_lifecycle::upsert_bucket_drain_exception(
                        &self.storage,
                        crate::mesh_lifecycle::BucketDrainExceptionInput {
                            tenant_id: tenant_id.clone(),
                            bucket_name: bucket_name.clone(),
                            region: region.to_string(),
                            disposition,
                            reason: reason.clone(),
                            expires_at: expires_at.clone(),
                        },
                    )
                    .await?;
                    exception_written = true;
                }
                crate::mesh_lifecycle::BucketDrainDisposition::DeleteAfterRetention => {
                    status_after = mesh_directory::BucketLocatorStatus::Draining;
                }
            }

            let mut generation_after = locator.generation;
            let mut locator_updated = false;
            if status_after != status_before {
                let mut updated = locator.clone();
                updated.status = status_after;
                updated.updated_at = Utc::now().to_rfc3339();
                updated.generation = updated.generation.saturating_add(1);
                self.write_mesh_bucket_locator_descriptor(&updated).await?;
                generation_after = updated.generation;
                locator_updated = true;
            }

            decisions.push(RegionDrainBucketDecision {
                tenant_id,
                bucket_name,
                bucket_locator_generation_before: locator.generation,
                bucket_locator_generation_after: generation_after,
                status_before,
                status_after,
                disposition,
                reason,
                expires_at,
                exception_written,
                locator_updated,
            });
        }

        Ok(RegionDrainPlanReport {
            region: region.to_string(),
            decisions,
        })
    }

    pub fn cache(&self) -> &MetadataCache {
        &self.cache
    }

    pub(super) async fn bucket_locators_in_region(
        &self,
        region: &str,
    ) -> Result<Vec<mesh_directory::BucketLocatorDescriptor>> {
        let records = mesh_directory::list_routing_records(
            &self.storage,
            Some(mesh_directory::RoutingRecordFamily::BucketLocator),
        )
        .await?;
        let mut locators = Vec::new();
        for record in records {
            let locator: mesh_directory::BucketLocatorDescriptor =
                serde_json::from_str(&record.payload_json)?;
            if locator.home_region.as_str() == region {
                locators.push(locator);
            }
        }
        Ok(locators)
    }

    pub(super) async fn write_mesh_bucket_locator_descriptor(
        &self,
        locator: &mesh_directory::BucketLocatorDescriptor,
    ) -> Result<()> {
        let permit = self
            .mesh_control_write_permit(
                mesh_directory::RoutingRecordFamily::BucketLocator,
                &locator.partition(),
            )
            .await?;
        mesh_directory::write_bucket_locator(
            &self.storage,
            locator,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await?;
        Ok(())
    }

    pub async fn move_bucket_home_region(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        target_region: &str,
    ) -> Result<Bucket> {
        let mut bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        if bucket.region == target_region {
            return Ok(bucket);
        }
        crate::mesh_lifecycle::ensure_region_accepts_new_writes(&self.storage, target_region)
            .await?;

        let target_cell = self
            .choose_bucket_home_cell(target_region)
            .await?
            .ok_or_else(|| anyhow!("target region has no active cell"))?;
        let tenant = mesh_directory::TenantId::new(tenant_id.to_string())?;
        let name = mesh_directory::BucketName::canonicalize(bucket_name)?;
        let key = mesh_directory::BucketLocatorKey::new(tenant, name);
        let existing = mesh_directory::read_bucket_locator(&self.storage, &key)
            .await?
            .ok_or_else(|| anyhow!("bucket locator not found"))?;

        let mut moved = existing.clone();
        moved.home_region = mesh_directory::RegionName::new(target_region.to_string())?;
        moved.home_cell = mesh_directory::CellId::new(target_cell)?;
        moved.status = mesh_directory::BucketLocatorStatus::Active;
        moved.updated_at = Utc::now().to_rfc3339();
        moved.generation = existing.generation.saturating_add(1);
        self.write_mesh_bucket_locator_descriptor(&moved).await?;

        bucket.region = target_region.to_string();
        let tenant_permit = self.bucket_tenant_write_permit(bucket.tenant_id).await?;
        let global_permit = self.bucket_global_write_permit().await?;
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &bucket,
            BucketJournalMutation::Update,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.cache.invalidate_bucket(tenant_id, bucket_name).await;
        self.publish_event(MetadataEvent::BucketUpdated {
            tenant_id,
            name: bucket_name.to_string(),
        })
        .await;
        Ok(bucket)
    }

    async fn choose_bucket_home_cell(&self, target_region: &str) -> Result<Option<String>> {
        let mut cells = crate::mesh_lifecycle::list_cells(&self.storage, Some(target_region))
            .await?
            .into_iter()
            .filter(|cell| cell.state == crate::mesh_lifecycle::LifecycleState::Active)
            .collect::<Vec<_>>();
        cells.sort_by(|left, right| {
            right
                .placement_weight
                .cmp(&left.placement_weight)
                .then_with(|| left.cell_id.cmp(&right.cell_id))
        });
        Ok(cells.into_iter().next().map(|cell| cell.cell_id))
    }

    pub(super) async fn global_write_permit(
        &self,
        partition_family: &str,
        partition_id: String,
    ) -> Result<PartitionWritePermit> {
        if self.partition_owner_signing_key.is_empty() {
            return Err(anyhow!("partition owner signing key must not be empty"));
        }
        self.ensure_owner_node_can_acquire_new_partition(partition_family)
            .await?;
        let _guard = partition_owner_acquire_lock(partition_family, &partition_id)
            .lock()
            .await;
        let mut last_cas_conflict = None;
        for attempt in 0..PARTITION_OWNER_ACQUIRE_ATTEMPTS {
            match self
                .global_write_permit_locked(partition_family, &partition_id)
                .await
            {
                Ok(permit) => return Ok(permit),
                Err(err) if is_partition_owner_cas_conflict(&err) => {
                    last_cas_conflict = Some(err);
                    let delay_ms = 1 + (attempt as u64 % 8);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_cas_conflict.unwrap_or_else(|| {
            anyhow!("CoreStore partition owner CAS conflict did not resolve after retries")
        }))
    }

    async fn global_write_permit_locked(
        &self,
        partition_family: &str,
        partition_id: &str,
    ) -> Result<PartitionWritePermit> {
        let now_nanos = Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("partition owner timestamp overflow"))?;
        if let Some(owner) = read_partition_owner(
            &self.storage,
            partition_family,
            partition_id,
            &self.partition_owner_signing_key,
        )
        .await?
        {
            if owner.owner_node_id != self.owner_node_id
                && !partition_owner_is_force_expired(&owner)
            {
                bail!(
                    "{OWNERSHIP_HELD}: partition {partition_family}/{partition_id} is owned by active node {}",
                    owner.owner_node_id
                );
            }
            if owner.status == PartitionOwnerStatus::Ready {
                return owner.write_permit().map_err(Into::into);
            }
            let ready = publish_partition_ready(
                &self.storage,
                partition_family,
                partition_id,
                &self.owner_node_id,
                owner.fence_token,
                owner.recovered_through_sequence,
                &owner.recovered_manifest_hash,
                now_nanos,
                &self.partition_owner_signing_key,
            )
            .await?;
            return ready.write_permit().map_err(Into::into);
        }

        let recovering = acquire_partition_recovery(
            &self.storage,
            PartitionRecoveryAcquire {
                partition_family: partition_family.to_string(),
                partition_id: partition_id.to_string(),
                owner_node_id: self.owner_node_id.clone(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await?;
        if recovering.status == PartitionOwnerStatus::Ready {
            return recovering.write_permit().map_err(Into::into);
        }
        let ready = publish_partition_ready(
            &self.storage,
            partition_family,
            partition_id,
            &self.owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([0; 32]),
            now_nanos.saturating_add(1),
            &self.partition_owner_signing_key,
        )
        .await?;
        ready.write_permit().map_err(Into::into)
    }

    pub(super) async fn ensure_owner_node_can_acquire_new_partition(
        &self,
        partition_family: &str,
    ) -> Result<()> {
        if matches!(
            partition_family,
            "control_plane" | mesh_directory::CONTROL_PARTITION_FAMILY
        ) {
            return Ok(());
        }
        let core_store = self.core_store().await?;
        let nodes = crate::mesh_lifecycle::list_nodes_with_core_store(
            &self.storage,
            &core_store,
            None,
            None,
        )
        .await
        .map_err(|err| anyhow!(err.to_string()))?;
        if nodes.is_empty() {
            return Ok(());
        }
        let Some(node) = nodes
            .into_iter()
            .find(|node| node.node_id == self.owner_node_id)
        else {
            return Ok(());
        };
        if node.state == crate::mesh_lifecycle::LifecycleState::Active {
            return Ok(());
        }
        Err(anyhow!(
            "node {} is {:?} and cannot acquire new partition ownership for {}",
            self.owner_node_id,
            node.state,
            partition_family
        ))
    }

    pub(super) async fn owned_write_permit(
        &self,
        resource_kind: OwnershipResourceKind,
        resource_id: String,
        partition_family: &str,
        partition_id: String,
    ) -> Result<PartitionWritePermit> {
        self.ensure_ownership_fence(resource_kind, resource_id)
            .await?;
        self.global_write_permit(partition_family, partition_id)
            .await
    }

    pub(crate) async fn ensure_ownership_fence(
        &self,
        resource_kind: OwnershipResourceKind,
        resource_id: String,
    ) -> Result<()> {
        let resource = OwnershipResource {
            resource_kind,
            resource_id,
        };
        let owner = self.ownership_principal();
        let now_nanos = Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("ownership timestamp overflow"))?;
        let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)?.saturating_mul(1_000_000);

        if let Some(record) = read_ownership_fence(
            &self.storage,
            owner.tenant_id,
            &resource,
            &self.partition_owner_signing_key,
        )
        .await?
        {
            if record.owner == owner && record.is_active_unexpired(now_nanos) {
                let remaining_nanos = record.lease_expires_at_nanos.saturating_sub(now_nanos);
                if remaining_nanos > ttl_nanos / 3 {
                    return Ok(());
                }
                renew_ownership(
                    &self.storage,
                    RenewOwnership {
                        request_id: format!(
                            "owned-write-renew-{}-{}",
                            resource.resource_kind.as_str(),
                            resource.resource_id
                        ),
                        resource: resource.clone(),
                        owner: owner.clone(),
                        current_fence: record.fence,
                        now_nanos,
                        ttl_nanos,
                    },
                    &self.partition_owner_signing_key,
                )
                .await?;
                return Ok(());
            }
        }

        acquire_ownership(
            &self.storage,
            AcquireOwnership {
                request_id: format!(
                    "owned-write-acquire-{}-{}",
                    resource.resource_kind.as_str(),
                    resource.resource_id
                ),
                idempotency_key: format!(
                    "owned-write-owner-{}-{}",
                    resource.resource_kind.as_str(),
                    resource.resource_id
                ),
                resource,
                owner,
                now_nanos,
                ttl_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn ensure_personaldb_group_ownership_fence(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<()> {
        self.ensure_ownership_fence(
            OwnershipResourceKind::PersonalDbGroup,
            format!("tenant/{tenant_id}/personaldb/{database_id}"),
        )
        .await
    }

    pub(crate) async fn ensure_index_build_ownership_fence(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_storage_id: &str,
    ) -> Result<()> {
        self.ensure_ownership_fence(
            OwnershipResourceKind::IndexPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/index_build/{index_storage_id}"),
        )
        .await
    }

    pub(super) async fn control_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "control_plane",
            hex::encode(control_journal::control_partition_id()),
        )
        .await
    }

    pub(super) async fn mesh_control_write_permit(
        &self,
        family: mesh_directory::RoutingRecordFamily,
        partition: &str,
    ) -> Result<PartitionWritePermit> {
        self.ensure_mesh_control_ownership(family, partition)
            .await?;
        self.global_write_permit(
            mesh_directory::CONTROL_PARTITION_FAMILY,
            mesh_directory::control_partition_id(family.stream_family(), partition),
        )
        .await
    }

    pub(super) async fn mesh_control_write_permit_for_stream(
        &self,
        stream_family: &str,
        partition: &str,
    ) -> Result<PartitionWritePermit> {
        self.ensure_mesh_control_stream_ownership(stream_family, partition)
            .await?;
        self.global_write_permit(
            mesh_directory::CONTROL_PARTITION_FAMILY,
            mesh_directory::control_partition_id(stream_family, partition),
        )
        .await
    }

    pub(super) async fn ensure_mesh_control_ownership(
        &self,
        family: mesh_directory::RoutingRecordFamily,
        partition: &str,
    ) -> Result<()> {
        self.ensure_mesh_control_stream_ownership(family.stream_family(), partition)
            .await
    }

    pub(super) async fn ensure_mesh_control_stream_ownership(
        &self,
        stream_family: &str,
        partition: &str,
    ) -> Result<()> {
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::ControlPartition,
            resource_id: format!("{stream_family}/{partition}"),
        };
        let owner = self.ownership_principal();
        let now_nanos = Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("ownership timestamp overflow"))?;
        let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)?.saturating_mul(1_000_000);

        if let Some(record) = read_ownership_fence(
            &self.storage,
            owner.tenant_id,
            &resource,
            &self.partition_owner_signing_key,
        )
        .await?
        {
            if record.owner == owner && record.is_active_unexpired(now_nanos) {
                renew_ownership(
                    &self.storage,
                    RenewOwnership {
                        request_id: format!("mesh-control-renew-{}", resource.resource_id),
                        resource: resource.clone(),
                        owner: owner.clone(),
                        current_fence: record.fence,
                        now_nanos,
                        ttl_nanos,
                    },
                    &self.partition_owner_signing_key,
                )
                .await?;
                return Ok(());
            }
        }

        acquire_ownership(
            &self.storage,
            AcquireOwnership {
                request_id: format!("mesh-control-acquire-{}", resource.resource_id),
                idempotency_key: format!("mesh-control-owner-{}", resource.resource_id),
                resource,
                owner,
                now_nanos,
                ttl_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await?;
        Ok(())
    }

    pub(super) fn ownership_principal(&self) -> OwnershipPrincipal {
        OwnershipPrincipal {
            tenant_id: 0,
            principal_kind: "node".to_string(),
            principal_id: self.owner_node_id.clone(),
            actor_instance_id: self.owner_node_id.clone(),
            display_name: self.owner_node_id.clone(),
            region: self.region.clone(),
            cell: self.cell_id.clone(),
        }
    }

    pub(super) async fn task_queue_write_permit(&self) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(task_journal::task_queue_partition_id());
        self.owned_write_permit(
            OwnershipResourceKind::TaskQueue,
            format!("task_queue/{partition_id}"),
            "task_queue",
            partition_id,
        )
        .await
    }

    pub(super) async fn model_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "model_metadata",
            hex::encode(model_journal::model_partition_id()),
        )
        .await
    }

    pub(super) async fn hf_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit("hf_metadata", hex::encode(hf_journal::hf_partition_id()))
            .await
    }

    pub(super) async fn bucket_tenant_write_permit(
        &self,
        tenant_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(bucket_journal::tenant_bucket_partition_id(tenant_id));
        self.owned_write_permit(
            OwnershipResourceKind::BucketPrimary,
            format!("tenant/{tenant_id}/bucket_metadata/{partition_id}"),
            "bucket_metadata",
            partition_id,
        )
        .await
    }

    pub(super) async fn bucket_global_write_permit(&self) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(bucket_journal::global_bucket_partition_id());
        self.owned_write_permit(
            OwnershipResourceKind::BucketPrimary,
            format!("global/bucket_metadata/{partition_id}"),
            "bucket_metadata",
            partition_id,
        )
        .await
    }

    pub(super) async fn object_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(metadata_journal::object_metadata_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::ObjectPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/object_metadata/{partition_id}"),
            "object_metadata",
            partition_id,
        )
        .await
    }

    pub(super) async fn multipart_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(multipart_journal::multipart_metadata_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::ObjectPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/multipart_metadata/{partition_id}"),
            "multipart_metadata",
            partition_id,
        )
        .await
    }

    pub(super) async fn append_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(append_journal::append_metadata_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::ObjectPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/append_metadata/{partition_id}"),
            "append_metadata",
            partition_id,
        )
        .await
    }

    pub(super) async fn manifest_cas_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(manifest_journal::manifest_cas_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::ObjectPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas/{partition_id}"),
            "manifest_cas",
            partition_id,
        )
        .await
    }

    pub(super) async fn authz_write_permit(&self, tenant_id: i64) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "authz_tuple",
            hex::encode(authz_journal::authz_partition_id(tenant_id)),
        )
        .await
    }

    pub(super) async fn repair_write_permit(
        &self,
        scope_kind: &str,
        scope_id: &str,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "repair",
            hex::encode(crate::formats::hash32(
                format!("repair\0{scope_kind}\0{scope_id}").as_bytes(),
            )),
        )
        .await
    }

    pub(super) async fn index_definition_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(index_journal::index_definition_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::IndexPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition/{partition_id}"),
            "index_definition",
            partition_id,
        )
        .await
    }

    pub(super) async fn index_diagnostic_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        let partition_id = hex::encode(index_diagnostic_journal::index_diagnostic_partition_id(
            tenant_id, bucket_id,
        ));
        self.owned_write_permit(
            OwnershipResourceKind::IndexPartition,
            format!("tenant/{tenant_id}/bucket/{bucket_id}/index_diagnostic/{partition_id}"),
            "index_diagnostic",
            partition_id,
        )
        .await
    }
}
