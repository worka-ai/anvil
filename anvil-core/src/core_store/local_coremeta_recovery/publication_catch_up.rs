use super::*;

impl CoreStore {
    /// Installs the publication which already owns the physical Q2 decision for
    /// an exact coordinator generation. Foreground optimistic conflicts use
    /// this bounded path so their caller can retry against the winning state
    /// without waiting for the steady-state anti-entropy interval.
    pub(in crate::core_store::local) async fn catch_up_committed_publication(
        &self,
        peers: &[RecoveryPeer],
        committed_coordinator_record: &[u8],
    ) -> Result<()> {
        let coordinator = decode_root_anchor_record(committed_coordinator_record)?;
        validate_root_anchor_record(&coordinator)?;
        let coordinator_scope = (
            coordinator.root_key_hash.clone(),
            coordinator.root_generation,
        );
        let coordinator_certificate_hash = coordinator
            .core_meta_commit_certificate_hash
            .as_deref()
            .ok_or_else(|| anyhow!("committed root-register anchor has no certificate"))?;
        let publication_bundle = self
            .stage_committed_generation(
                peers,
                &coordinator.root_key_hash,
                coordinator.root_generation,
                coordinator_certificate_hash,
                None,
            )
            .await?;
        let plan = decode_coremeta_recovery_publication_bundle(&publication_bundle)?;
        if plan.coordinator_scope != coordinator_scope {
            bail!(
                "committed root-register anchor does not match its CoreMeta publication coordinator"
            );
        }
        validate_recovery_publication_anchor(
            &plan,
            &coordinator_scope,
            committed_coordinator_record,
        )?;

        let mut committed_anchors = BTreeMap::new();
        committed_anchors.insert(
            coordinator_scope.clone(),
            committed_coordinator_record.to_vec(),
        );
        for scope in &plan.scopes {
            if scope == &coordinator_scope {
                continue;
            }
            let anchor = self
                .fetch_committed_cache_anchor(peers, &scope.0, scope.1)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "committed CoreMeta publication participant has no root-cache quorum: root={} generation={}",
                        scope.0,
                        scope.1
                    )
                })?;
            validate_recovery_publication_anchor(&plan, scope, &anchor)?;
            committed_anchors.insert(scope.clone(), anchor);
        }

        for scope in &plan.scopes {
            let anchor = committed_anchors
                .get(scope)
                .ok_or_else(|| anyhow!("committed CoreMeta publication anchor is missing"))?;
            let anchor = decode_root_anchor_record(anchor)?;
            let certificate_hash = anchor
                .core_meta_commit_certificate_hash
                .as_deref()
                .ok_or_else(|| anyhow!("committed CoreMeta participant has no certificate"))?;
            let staged_bundle = self
                .stage_committed_generation(
                    peers,
                    &scope.0,
                    scope.1,
                    certificate_hash,
                    Some(&publication_bundle),
                )
                .await?;
            if staged_bundle != publication_bundle {
                bail!("committed CoreMeta publication participants disagree on their bundle");
            }
        }

        self.publish_staged_coremeta_recovery_bundle(&publication_bundle, &committed_anchors)
            .await?;
        let transaction_id = publication_transaction_id(&coordinator)?;
        for (root_key_hash, generation) in &plan.scopes {
            if !self.root_generation_is_published(root_key_hash, *generation, &transaction_id)? {
                bail!(
                    "committed CoreMeta publication catch-up did not make every participant visible"
                );
            }
        }
        Ok(())
    }

    async fn stage_committed_generation(
        &self,
        peers: &[RecoveryPeer],
        root_key_hash: &str,
        generation: u64,
        committed_certificate_hash: &str,
        committed_publication_bundle: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        if let Some(prepared) =
            self.read_complete_coremeta_generation_for_recovery(root_key_hash, generation)?
            && prepared.descriptor.certificate_hash == committed_certificate_hash
            && committed_publication_bundle
                .is_none_or(|expected| prepared.descriptor.publication_bundle == expected)
        {
            return Ok(prepared.descriptor.publication_bundle);
        }

        let local_generation = self.coremeta_recovery_published_generation(root_key_hash)?;
        if local_generation.saturating_add(1) != generation {
            bail!(
                "foreground CoreMeta catch-up requires the next generation: root={root_key_hash} local={local_generation} target={generation}"
            );
        }
        let mut round = RecoveryRound::default();
        let sources = self
            .coremeta_recovery_sources(peers, root_key_hash, &mut round)
            .await?;
        let candidates = sources.iter().filter(|source| {
            source.final_generation >= generation && source.retention_floor_generation <= generation
        });
        let mut failures = Vec::new();
        for source in candidates {
            match self
                .fetch_committed_coremeta_generation(
                    source,
                    root_key_hash,
                    generation,
                    committed_certificate_hash,
                    committed_publication_bundle,
                )
                .await
            {
                Ok(bundle) => return Ok(bundle),
                Err(error) => failures.push(format!("{}: {error:#}", source.peer.node_id)),
            }
        }
        bail!(
            "foreground CoreMeta catch-up has no source for the committed generation: root={root_key_hash} generation={generation}: {}",
            failures.join("; ")
        )
    }
}
