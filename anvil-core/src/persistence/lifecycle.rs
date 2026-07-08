use super::*;

impl Persistence {
    pub async fn create_region(&self, name: &str) -> Result<bool> {
        let permit = self.control_write_permit().await?;
        control_journal::create_region_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_regions(&self) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .regions())
    }

    pub async fn create_region_descriptor(
        &self,
        input: crate::mesh_lifecycle::CreateRegionDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            &input.region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::create_region_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn transition_region_descriptor(
        &self,
        region: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_region_with_control(
            &self.storage,
            region,
            expected_generation,
            target,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn activate_region_descriptor(
        &self,
        region: &str,
        expected_generation: u64,
        checkpoint: &crate::mesh_lifecycle::ActivationCheckpoint,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::activate_region_with_control(
            &self.storage,
            region,
            expected_generation,
            checkpoint,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn list_region_descriptors(
        &self,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::RegionDescriptor>> {
        crate::mesh_lifecycle::list_regions(&self.storage).await
    }

    pub async fn register_cell_descriptor(
        &self,
        input: crate::mesh_lifecycle::RegisterCellDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::CellDescriptor> {
        let record_key = format!("{}/{}", input.region, input.cell_id);
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::register_cell_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn transition_cell_descriptor(
        &self,
        region: &str,
        cell_id: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::CellDescriptor> {
        let record_key = format!("{region}/{cell_id}");
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_cell_with_control(
            &self.storage,
            region,
            cell_id,
            expected_generation,
            target,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn list_cell_descriptors(
        &self,
        region_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::CellDescriptor>> {
        crate::mesh_lifecycle::list_cells(&self.storage, region_filter).await
    }

    pub async fn register_node_descriptor(
        &self,
        input: crate::mesh_lifecycle::RegisterNodeDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::NodeDescriptor> {
        let record_key = format!("{}/{}/{}", input.region, input.cell_id, input.node_id);
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::register_node_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn transition_node_descriptor(
        &self,
        node_id: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
        drain: Option<crate::mesh_lifecycle::NodeDrainDescriptor>,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::NodeDescriptor> {
        let node = crate::mesh_lifecycle::list_nodes(&self.storage, None, None)
            .await?
            .into_iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| crate::mesh_lifecycle::LifecycleError::NotFound {
                resource_kind: "node",
                resource_id: node_id.to_string(),
            })?;
        if node.generation != expected_generation {
            return Err(crate::mesh_lifecycle::LifecycleError::GenerationConflict {
                resource_kind: "node",
                resource_id: node_id.to_string(),
                expected: expected_generation,
                current: node.generation,
            });
        }
        crate::mesh_lifecycle::validate_node_transition(node.state, target).map_err(|_| {
            crate::mesh_lifecycle::LifecycleError::LifecycleTransitionDenied {
                resource_kind: "node",
                resource_id: node_id.to_string(),
                from: node.state,
                to: target,
            }
        })?;
        match target {
            crate::mesh_lifecycle::LifecycleState::Drained => {
                self.ensure_node_has_no_runtime_ownership(node_id).await?;
            }
            crate::mesh_lifecycle::LifecycleState::Offline
            | crate::mesh_lifecycle::LifecycleState::Removed => {
                self.force_expire_node_runtime_ownership(node_id).await?;
            }
            _ => {}
        }
        let record_key = format!("{}/{}/{}", node.region, node.cell_id, node.node_id);
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_node_with_control(
            &self.storage,
            node_id,
            expected_generation,
            target,
            drain,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn node_runtime_ownership_blockers(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<String>> {
        let now_nanos = current_time_nanos().map_err(|err| {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
        })?;
        self.node_runtime_ownership_blockers_at(node_id, now_nanos)
            .await
    }

    pub(super) async fn ensure_node_has_no_runtime_ownership(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<()> {
        let blockers = self.node_runtime_ownership_blockers(node_id).await?;
        if blockers.is_empty() {
            return Ok(());
        }
        Err(crate::mesh_lifecycle::LifecycleError::InvalidArgument(
            format!(
                "node {node_id} drain cannot complete: {} runtime ownership record(s) still exist: {}",
                blockers.len(),
                blockers.join(", ")
            ),
        ))
    }

    pub(super) async fn node_runtime_ownership_blockers_at(
        &self,
        node_id: &str,
        now_nanos: i64,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<String>> {
        let mut blockers = Vec::new();
        let partition_owners = list_partition_owners_for_node(
            &self.storage,
            node_id,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        blockers.extend(partition_owners.into_iter().map(|owner| {
            format!(
                "partition_owner:{}/{}:{:?}:fence={}",
                owner.partition_family, owner.partition_id, owner.status, owner.fence_token
            )
        }));

        let ownership_fences = list_active_ownership_fences_for_node(
            &self.storage,
            node_id,
            now_nanos,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        blockers.extend(ownership_fences.into_iter().map(|record| {
            format!(
                "ownership_fence:{}/{}:{:?}:fence={}",
                record.resource.resource_kind.as_str(),
                record.resource.resource_id,
                record.state,
                record.fence
            )
        }));

        let task_leases = task_lease::list_active_task_leases_for_node(
            &self.storage,
            node_id,
            now_nanos,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        blockers.extend(task_leases.into_iter().map(|lease| {
            format!(
                "task_lease:{}:{}:fence={}",
                lease.task_kind, lease.task_id, lease.fence_token
            )
        }));
        blockers.sort();
        Ok(blockers)
    }

    pub(super) async fn force_expire_node_runtime_ownership(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<()> {
        let now_nanos = current_time_nanos().map_err(|err| {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
        })?;
        let partition_owners = list_partition_owners_for_node(
            &self.storage,
            node_id,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        for owner in partition_owners {
            force_expire_partition_owner_for_node(
                &self.storage,
                &owner.partition_family,
                &owner.partition_id,
                node_id,
                now_nanos,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        }

        let ownership_fences = list_active_ownership_fences_for_node(
            &self.storage,
            node_id,
            now_nanos,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        let admin = OwnershipPrincipal {
            tenant_id: 0,
            principal_kind: "node_admin".to_string(),
            principal_id: self.owner_node_id.clone(),
            actor_instance_id: self.owner_node_id.clone(),
            display_name: self.owner_node_id.clone(),
            region: self.region.clone(),
            cell: self.cell_id.clone(),
        };
        for record in ownership_fences {
            let mut admin = admin.clone();
            admin.tenant_id = record.owner.tenant_id;
            force_expire_ownership(
                &self.storage,
                ForceExpireOwnership {
                    request_id: format!(
                        "node-force-expire-{}-{}",
                        node_id,
                        record.resource.resource_id.replace('/', "-")
                    ),
                    idempotency_key: format!(
                        "node-force-expire-{}-{}-{}",
                        node_id, record.resource.resource_id, record.fence
                    ),
                    resource: record.resource,
                    admin: admin.clone(),
                    reason: format!("node {node_id} transitioned to non-owning lifecycle state"),
                    now_nanos,
                },
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        }

        let task_leases = task_lease::list_active_task_leases_for_node(
            &self.storage,
            node_id,
            now_nanos,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|err| crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string()))?;
        for lease in task_leases {
            task_lease::force_release_task_lease(
                &self.storage,
                lease.owner.tenant_id,
                &lease.task_id,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        }
        Ok(())
    }

    pub async fn list_node_descriptors(
        &self,
        region_filter: Option<&str>,
        cell_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::NodeDescriptor>> {
        crate::mesh_lifecycle::list_nodes(&self.storage, region_filter, cell_filter).await
    }

    pub async fn create_host_alias_descriptor(
        &self,
        routing_config: &crate::routing::RoutingConfig,
        input: crate::mesh_lifecycle::CreateHostAliasDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor =
            crate::mesh_lifecycle::create_host_alias(&self.storage, routing_config, input).await?;
        let partition = mesh_directory::host_alias_partition(&descriptor.hostname)
            .map_err(mesh_directory_lifecycle_error)?;
        let permit = self
            .mesh_control_write_permit(mesh_directory::RoutingRecordFamily::HostAlias, &partition)
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        mesh_directory::write_host_alias_descriptor(
            &self.storage,
            &descriptor,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn transition_host_alias_descriptor(
        &self,
        hostname: &str,
        expected_generation: u64,
        target: crate::routing::HostAliasState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor = crate::mesh_lifecycle::transition_host_alias(
            &self.storage,
            hostname,
            expected_generation,
            target,
        )
        .await?;
        let partition = mesh_directory::host_alias_partition(&descriptor.hostname)
            .map_err(mesh_directory_lifecycle_error)?;
        let permit = self
            .mesh_control_write_permit(mesh_directory::RoutingRecordFamily::HostAlias, &partition)
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        mesh_directory::write_host_alias_descriptor(
            &self.storage,
            &descriptor,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn get_host_alias_descriptor(
        &self,
        hostname: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<Option<crate::routing::HostAliasDescriptor>> {
        mesh_directory::read_host_alias_descriptor(&self.storage, hostname)
            .await
            .map_err(mesh_directory_lifecycle_error)
    }

    pub async fn list_host_alias_descriptors(
        &self,
        region_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::routing::HostAliasDescriptor>> {
        crate::mesh_lifecycle::list_host_aliases(&self.storage, region_filter).await
    }
}
