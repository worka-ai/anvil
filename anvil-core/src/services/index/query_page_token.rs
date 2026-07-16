use super::*;
use base64::Engine;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct IndexPageTokenProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    token_kind: String,
    #[prost(string, tag = "3")]
    mesh_id: String,
    #[prost(string, tag = "4")]
    anvil_storage_tenant_id: String,
    #[prost(string, tag = "5")]
    authz_realm_id: String,
    #[prost(int64, tag = "6")]
    tenant_id: i64,
    #[prost(string, tag = "7")]
    bucket_name: String,
    #[prost(string, tag = "8")]
    index_name: String,
    #[prost(uint64, tag = "9")]
    index_generation: u64,
    #[prost(uint64, tag = "10")]
    index_definition_version: u64,
    #[prost(message, repeated, tag = "11")]
    index_inputs: Vec<IndexPageTokenInputProto>,
    #[prost(uint64, tag = "12")]
    authz_revision: u64,
    #[prost(string, tag = "13")]
    caller_principal_hash: String,
    #[prost(string, tag = "14")]
    query_hash: String,
    #[prost(string, tag = "15")]
    predicate_hash: String,
    #[prost(string, tag = "16")]
    order_hash: String,
    #[prost(string, tag = "17")]
    last_source_identity: String,
    #[prost(message, repeated, tag = "18")]
    last_sort_values: Vec<IndexPageTokenJsonEntryProto>,
    #[prost(string, tag = "19")]
    expires_at: String,
    #[prost(string, tag = "20")]
    authz_scope_hash: String,
    #[prost(string, tag = "21")]
    object_namespace: String,
    #[prost(string, tag = "22")]
    relation: String,
    #[prost(string, tag = "23")]
    boundary_schema_generation_hash: String,
    #[prost(string, tag = "24")]
    signature: String,
    #[prost(uint64, tag = "25")]
    root_generation: u64,
    #[prost(string, tag = "26")]
    root_key_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexPageTokenInputProto {
    #[prost(string, tag = "1")]
    index_id: String,
    #[prost(string, tag = "2")]
    definition_hash: String,
    #[prost(uint64, tag = "3")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexPageTokenJsonEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(bytes, tag = "2")]
    canonical_json: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct IndexPageToken {
    pub(super) version: u8,
    pub(super) token_kind: String,
    pub(super) mesh_id: String,
    pub(super) anvil_storage_tenant_id: String,
    pub(super) authz_realm_id: String,
    pub(super) tenant_id: i64,
    pub(super) bucket_name: String,
    pub(super) index_name: String,
    pub(super) index_generation: u64,
    pub(super) root_generation: u64,
    pub(super) root_key_hash: String,
    pub(super) index_definition_version: u64,
    pub(super) index_inputs: Vec<IndexPageTokenInput>,
    pub(super) authz_revision: u64,
    pub(super) caller_principal_hash: String,
    pub(super) query_hash: String,
    pub(super) predicate_hash: String,
    pub(super) order_hash: String,
    pub(super) last_source_identity: String,
    #[serde(default)]
    pub(super) last_sort_values: BTreeMap<String, JsonValue>,
    pub(super) expires_at: String,
    pub(super) authz_scope_hash: String,
    pub(super) object_namespace: String,
    pub(super) relation: String,
    pub(super) boundary_schema_generation_hash: String,
    pub(super) signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct IndexPageTokenInput {
    pub(super) index_id: String,
    pub(super) definition_hash: String,
    pub(super) generation: u64,
}

#[derive(Debug, Clone)]
pub(super) struct IndexPageTokenBinding {
    pub(super) token_kind: String,
    pub(super) mesh_id: String,
    pub(super) anvil_storage_tenant_id: String,
    pub(super) authz_realm_id: String,
    pub(super) tenant_id: i64,
    pub(super) bucket_name: String,
    pub(super) index_name: String,
    pub(super) index_generation: u64,
    pub(super) root_generation: u64,
    pub(super) root_key_hash: String,
    pub(super) index_definition_version: u64,
    pub(super) index_inputs: Vec<IndexPageTokenInput>,
    pub(super) authz_revision: u64,
    pub(super) caller_principal_hash: String,
    pub(super) query_hash: String,
    pub(super) predicate_hash: String,
    pub(super) order_hash: String,
    pub(super) authz_scope_hash: String,
    pub(super) object_namespace: String,
    pub(super) relation: String,
    pub(super) boundary_schema_generation_hash: String,
}

impl IndexPageToken {
    pub(super) fn for_cursor(
        binding: &IndexPageTokenBinding,
        last_source_identity: String,
        last_sort_values: BTreeMap<String, JsonValue>,
    ) -> Self {
        Self {
            version: INDEX_PAGE_TOKEN_VERSION,
            token_kind: binding.token_kind.clone(),
            mesh_id: binding.mesh_id.clone(),
            anvil_storage_tenant_id: binding.anvil_storage_tenant_id.clone(),
            authz_realm_id: binding.authz_realm_id.clone(),
            tenant_id: binding.tenant_id,
            bucket_name: binding.bucket_name.clone(),
            index_name: binding.index_name.clone(),
            index_generation: binding.index_generation,
            root_generation: binding.root_generation,
            root_key_hash: binding.root_key_hash.clone(),
            index_definition_version: binding.index_definition_version,
            index_inputs: binding.index_inputs.clone(),
            authz_revision: binding.authz_revision,
            caller_principal_hash: binding.caller_principal_hash.clone(),
            query_hash: binding.query_hash.clone(),
            predicate_hash: binding.predicate_hash.clone(),
            order_hash: binding.order_hash.clone(),
            last_source_identity,
            last_sort_values,
            expires_at: (chrono::Utc::now()
                + chrono::Duration::seconds(INDEX_PAGE_TOKEN_TTL_SECONDS))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            authz_scope_hash: binding.authz_scope_hash.clone(),
            object_namespace: binding.object_namespace.clone(),
            relation: binding.relation.clone(),
            boundary_schema_generation_hash: binding.boundary_schema_generation_hash.clone(),
            signature: String::new(),
        }
    }

    pub(super) fn decode(raw: &str, signing_key: &[u8]) -> Result<Option<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        let token = index_page_token_from_proto(
            decode_page_token_proto(&bytes)
                .map_err(|_| Status::invalid_argument("InvalidPageToken"))?,
        )?;
        if token.version != INDEX_PAGE_TOKEN_VERSION {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        let expected = token.sign(signing_key)?;
        if !constant_time_eq::constant_time_eq(token.signature.as_bytes(), expected.as_bytes()) {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
        token
            .validate_scope_hashes()
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        Ok(Some(token))
    }

    pub(super) fn encode(mut self, signing_key: &[u8]) -> Result<String, Status> {
        self.signature = self.sign(signing_key)?;
        let bytes = encode_page_token_proto(&self)?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    pub(super) fn validate(&self, binding: &IndexPageTokenBinding) -> Result<(), Status> {
        let expires_at = chrono::DateTime::parse_from_rfc3339(&self.expires_at)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?
            .with_timezone(&chrono::Utc);
        if expires_at <= chrono::Utc::now() {
            return Err(Status::invalid_argument("PageTokenExpired"));
        }
        self.validate_scope_hashes()
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?;
        binding.validate_scope_hashes()?;
        if self.token_kind != binding.token_kind
            || self.mesh_id != binding.mesh_id
            || self.anvil_storage_tenant_id != binding.anvil_storage_tenant_id
            || self.authz_realm_id != binding.authz_realm_id
            || self.tenant_id != binding.tenant_id
            || self.bucket_name != binding.bucket_name
            || self.index_name != binding.index_name
            || self.index_generation != binding.index_generation
            || self.root_generation != binding.root_generation
            || self.root_key_hash != binding.root_key_hash
            || self.index_definition_version != binding.index_definition_version
            || self.index_inputs != binding.index_inputs
            || self.authz_revision != binding.authz_revision
            || self.caller_principal_hash != binding.caller_principal_hash
            || self.query_hash != binding.query_hash
            || self.predicate_hash != binding.predicate_hash
            || self.order_hash != binding.order_hash
            || self.authz_scope_hash != binding.authz_scope_hash
            || self.object_namespace != binding.object_namespace
            || self.relation != binding.relation
            || self.boundary_schema_generation_hash != binding.boundary_schema_generation_hash
        {
            return Err(Status::invalid_argument("PageTokenScopeMismatch"));
        }
        Ok(())
    }

    pub(super) fn sign(&self, signing_key: &[u8]) -> Result<String, Status> {
        let mut mac = HmacSha256::new_from_slice(signing_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))?;
        mac.update(INDEX_PAGE_TOKEN_DOMAIN);
        mac.update(&[self.version]);
        update_mac_part(&mut mac, self.token_kind.as_bytes());
        update_mac_part(&mut mac, self.mesh_id.as_bytes());
        update_mac_part(&mut mac, self.anvil_storage_tenant_id.as_bytes());
        update_mac_part(&mut mac, self.authz_realm_id.as_bytes());
        mac.update(&self.tenant_id.to_le_bytes());
        update_mac_part(&mut mac, self.bucket_name.as_bytes());
        update_mac_part(&mut mac, self.index_name.as_bytes());
        mac.update(&self.index_generation.to_le_bytes());
        mac.update(&self.root_generation.to_le_bytes());
        update_mac_part(&mut mac, self.root_key_hash.as_bytes());
        mac.update(&self.index_definition_version.to_le_bytes());
        let index_inputs = encode_page_token_inputs(&self.index_inputs)?;
        update_mac_part(&mut mac, &index_inputs);
        mac.update(&self.authz_revision.to_le_bytes());
        update_mac_part(&mut mac, self.caller_principal_hash.as_bytes());
        update_mac_part(&mut mac, self.query_hash.as_bytes());
        update_mac_part(&mut mac, self.predicate_hash.as_bytes());
        update_mac_part(&mut mac, self.order_hash.as_bytes());
        update_mac_part(&mut mac, self.last_source_identity.as_bytes());
        let sort_values = encode_page_token_json_entries(&self.last_sort_values)?;
        update_mac_part(&mut mac, &sort_values);
        update_mac_part(&mut mac, self.expires_at.as_bytes());
        update_mac_part(&mut mac, self.authz_scope_hash.as_bytes());
        update_mac_part(&mut mac, self.object_namespace.as_bytes());
        update_mac_part(&mut mac, self.relation.as_bytes());
        update_mac_part(&mut mac, self.boundary_schema_generation_hash.as_bytes());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn validate_scope_hashes(&self) -> Result<(), Status> {
        ensure_algorithm_prefixed_hash(&self.root_key_hash, "root_key_hash")?;
        ensure_algorithm_prefixed_hash(&self.caller_principal_hash, "caller_principal_hash")?;
        ensure_algorithm_prefixed_hash(&self.query_hash, "query_hash")?;
        ensure_algorithm_prefixed_hash(&self.predicate_hash, "predicate_hash")?;
        ensure_algorithm_prefixed_hash(&self.order_hash, "order_hash")?;
        ensure_algorithm_prefixed_hash(&self.authz_scope_hash, "authz_scope_hash")?;
        ensure_algorithm_prefixed_hash(
            &self.boundary_schema_generation_hash,
            "boundary_schema_generation_hash",
        )?;
        for input in &self.index_inputs {
            ensure_algorithm_prefixed_hash(&input.definition_hash, "index_definition_hash")?;
        }
        Ok(())
    }
}

fn encode_page_token_proto(token: &IndexPageToken) -> Result<Vec<u8>, Status> {
    let proto = index_page_token_to_proto(token, true)?;
    Ok(crate::core_store::encode_deterministic_proto(&proto))
}

fn decode_page_token_proto(bytes: &[u8]) -> Result<IndexPageTokenProto, anyhow::Error> {
    crate::core_store::decode_deterministic_proto::<IndexPageTokenProto>(bytes, "index page token")
}

fn index_page_token_to_proto(
    token: &IndexPageToken,
    include_signature: bool,
) -> Result<IndexPageTokenProto, Status> {
    Ok(IndexPageTokenProto {
        version: u32::from(token.version),
        token_kind: token.token_kind.clone(),
        mesh_id: token.mesh_id.clone(),
        anvil_storage_tenant_id: token.anvil_storage_tenant_id.clone(),
        authz_realm_id: token.authz_realm_id.clone(),
        tenant_id: token.tenant_id,
        bucket_name: token.bucket_name.clone(),
        index_name: token.index_name.clone(),
        index_generation: token.index_generation,
        root_generation: token.root_generation,
        root_key_hash: token.root_key_hash.clone(),
        index_definition_version: token.index_definition_version,
        index_inputs: token
            .index_inputs
            .iter()
            .map(index_page_token_input_to_proto)
            .collect(),
        authz_revision: token.authz_revision,
        caller_principal_hash: token.caller_principal_hash.clone(),
        query_hash: token.query_hash.clone(),
        predicate_hash: token.predicate_hash.clone(),
        order_hash: token.order_hash.clone(),
        last_source_identity: token.last_source_identity.clone(),
        last_sort_values: json_map_to_token_entries(&token.last_sort_values)?,
        expires_at: token.expires_at.clone(),
        authz_scope_hash: token.authz_scope_hash.clone(),
        object_namespace: token.object_namespace.clone(),
        relation: token.relation.clone(),
        boundary_schema_generation_hash: token.boundary_schema_generation_hash.clone(),
        signature: if include_signature {
            token.signature.clone()
        } else {
            String::new()
        },
    })
}

fn index_page_token_from_proto(proto: IndexPageTokenProto) -> Result<IndexPageToken, Status> {
    Ok(IndexPageToken {
        version: u8::try_from(proto.version)
            .map_err(|_| Status::invalid_argument("InvalidPageToken"))?,
        token_kind: proto.token_kind,
        mesh_id: proto.mesh_id,
        anvil_storage_tenant_id: proto.anvil_storage_tenant_id,
        authz_realm_id: proto.authz_realm_id,
        tenant_id: proto.tenant_id,
        bucket_name: proto.bucket_name,
        index_name: proto.index_name,
        index_generation: proto.index_generation,
        root_generation: proto.root_generation,
        root_key_hash: proto.root_key_hash,
        index_definition_version: proto.index_definition_version,
        index_inputs: proto
            .index_inputs
            .into_iter()
            .map(index_page_token_input_from_proto)
            .collect(),
        authz_revision: proto.authz_revision,
        caller_principal_hash: proto.caller_principal_hash,
        query_hash: proto.query_hash,
        predicate_hash: proto.predicate_hash,
        order_hash: proto.order_hash,
        last_source_identity: proto.last_source_identity,
        last_sort_values: token_entries_to_json_map(proto.last_sort_values)?,
        expires_at: proto.expires_at,
        authz_scope_hash: proto.authz_scope_hash,
        object_namespace: proto.object_namespace,
        relation: proto.relation,
        boundary_schema_generation_hash: proto.boundary_schema_generation_hash,
        signature: proto.signature,
    })
}

fn index_page_token_input_to_proto(input: &IndexPageTokenInput) -> IndexPageTokenInputProto {
    IndexPageTokenInputProto {
        index_id: input.index_id.clone(),
        definition_hash: input.definition_hash.clone(),
        generation: input.generation,
    }
}

fn index_page_token_input_from_proto(proto: IndexPageTokenInputProto) -> IndexPageTokenInput {
    IndexPageTokenInput {
        index_id: proto.index_id,
        definition_hash: proto.definition_hash,
        generation: proto.generation,
    }
}

fn encode_page_token_inputs(inputs: &[IndexPageTokenInput]) -> Result<Vec<u8>, Status> {
    #[derive(Clone, PartialEq, Message)]
    struct InputListProto {
        #[prost(message, repeated, tag = "1")]
        inputs: Vec<IndexPageTokenInputProto>,
    }
    Ok(crate::core_store::encode_deterministic_proto(
        &InputListProto {
            inputs: inputs.iter().map(index_page_token_input_to_proto).collect(),
        },
    ))
}

fn encode_page_token_json_entries(values: &BTreeMap<String, JsonValue>) -> Result<Vec<u8>, Status> {
    #[derive(Clone, PartialEq, Message)]
    struct EntryListProto {
        #[prost(message, repeated, tag = "1")]
        entries: Vec<IndexPageTokenJsonEntryProto>,
    }
    Ok(crate::core_store::encode_deterministic_proto(
        &EntryListProto {
            entries: json_map_to_token_entries(values)?,
        },
    ))
}

fn json_map_to_token_entries(
    values: &BTreeMap<String, JsonValue>,
) -> Result<Vec<IndexPageTokenJsonEntryProto>, Status> {
    values
        .iter()
        .map(|(key, value)| {
            Ok(IndexPageTokenJsonEntryProto {
                key: key.clone(),
                canonical_json: page_token_canonical_json_bytes(value)?,
            })
        })
        .collect()
}

fn token_entries_to_json_map(
    entries: Vec<IndexPageTokenJsonEntryProto>,
) -> Result<BTreeMap<String, JsonValue>, Status> {
    let mut values = BTreeMap::new();
    for entry in entries {
        if values
            .insert(
                entry.key,
                serde_json::from_slice(&entry.canonical_json)
                    .map_err(|_| Status::invalid_argument("InvalidPageToken"))?,
            )
            .is_some()
        {
            return Err(Status::invalid_argument("InvalidPageToken"));
        }
    }
    Ok(values)
}

fn page_token_canonical_json_bytes(value: &JsonValue) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(&page_token_canonical_json(value))
        .map_err(|e| Status::internal(format!("Serialize page token JSON value: {e}")))
}

pub(super) fn page_token_canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(page_token_canonical_json).collect())
        }
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), page_token_canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

impl IndexPageTokenBinding {
    pub(super) fn single_index(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        root_generation: u64,
        index_definition_version: u64,
        authz_revision: u64,
        authz_scope: &QueryAuthzScope,
        predicate_hash: String,
        order_hash: String,
        boundary_schema_generation_hash: String,
    ) -> Self {
        let index_inputs = vec![IndexPageTokenInput {
            index_id: index_name.to_string(),
            definition_hash: stable_string_hash(&format!(
                "{index_name}:{index_definition_version}"
            )),
            generation: index_generation,
        }];
        Self::with_index_inputs(
            config,
            claims,
            token_kind,
            bucket_name,
            index_name,
            index_generation,
            root_generation,
            index_definition_version,
            index_inputs,
            authz_revision,
            authz_scope,
            predicate_hash,
            order_hash,
            boundary_schema_generation_hash,
        )
    }

    pub(super) fn with_index_inputs(
        config: &Config,
        claims: &auth::Claims,
        token_kind: &str,
        bucket_name: &str,
        index_name: &str,
        index_generation: u64,
        root_generation: u64,
        index_definition_version: u64,
        index_inputs: Vec<IndexPageTokenInput>,
        authz_revision: u64,
        authz_scope: &QueryAuthzScope,
        predicate_hash: String,
        order_hash: String,
        boundary_schema_generation_hash: String,
    ) -> Self {
        let anvil_storage_tenant_id = claims.tenant_id.to_string();
        let authz_realm_id = authz_scope.realm_id.clone();
        let caller_principal_hash = stable_string_hash(&claims.sub);
        let root_key_hash = stable_prefixed_json_hash(&serde_json::json!({
            "schema": "anvil.query.root_key.v1",
            "mesh_id": config.mesh_id.clone(),
            "anvil_storage_tenant_id": anvil_storage_tenant_id.clone(),
            "tenant_id": claims.tenant_id,
            "bucket_name": bucket_name,
            "index_name": index_name,
            "authz_realm_id": authz_realm_id.clone(),
            "authz_scope_hash": authz_scope.scope_hash.clone(),
            "object_namespace": authz_scope.object_namespace.clone(),
            "relation": authz_scope.relation.clone(),
            "caller_principal_hash": caller_principal_hash.clone(),
        }));
        let query_hash = stable_prefixed_json_hash(&serde_json::json!({
            "schema": "anvil.query.page_token_scope.v1",
            "token_kind": token_kind,
            "mesh_id": config.mesh_id.clone(),
            "anvil_storage_tenant_id": anvil_storage_tenant_id.clone(),
            "authz_realm_id": authz_realm_id.clone(),
            "tenant_id": claims.tenant_id,
            "bucket_name": bucket_name,
            "index_name": index_name,
            "index_generation": index_generation,
            "root_generation": root_generation,
            "root_key_hash": root_key_hash.clone(),
            "index_definition_version": index_definition_version,
            "index_inputs": index_inputs.clone(),
            "authz_revision": authz_revision,
            "authz_scope_hash": authz_scope.scope_hash.clone(),
            "object_namespace": authz_scope.object_namespace.clone(),
            "relation": authz_scope.relation.clone(),
            "caller_principal_hash": caller_principal_hash.clone(),
            "predicate_hash": predicate_hash.clone(),
            "order_hash": order_hash.clone(),
            "boundary_schema_generation_hash": boundary_schema_generation_hash.clone(),
        }));
        Self {
            token_kind: token_kind.to_string(),
            mesh_id: config.mesh_id.clone(),
            anvil_storage_tenant_id,
            authz_realm_id,
            tenant_id: claims.tenant_id,
            bucket_name: bucket_name.to_string(),
            index_name: index_name.to_string(),
            index_generation,
            root_generation,
            root_key_hash,
            index_definition_version,
            index_inputs,
            authz_revision,
            caller_principal_hash,
            query_hash,
            predicate_hash,
            order_hash,
            authz_scope_hash: authz_scope.scope_hash.clone(),
            object_namespace: authz_scope.object_namespace.clone(),
            relation: authz_scope.relation.clone(),
            boundary_schema_generation_hash,
        }
    }

    fn validate_scope_hashes(&self) -> Result<(), Status> {
        ensure_algorithm_prefixed_hash(&self.root_key_hash, "root_key_hash")?;
        ensure_algorithm_prefixed_hash(&self.caller_principal_hash, "caller_principal_hash")?;
        ensure_algorithm_prefixed_hash(&self.query_hash, "query_hash")?;
        ensure_algorithm_prefixed_hash(&self.predicate_hash, "predicate_hash")?;
        ensure_algorithm_prefixed_hash(&self.order_hash, "order_hash")?;
        ensure_algorithm_prefixed_hash(&self.authz_scope_hash, "authz_scope_hash")?;
        ensure_algorithm_prefixed_hash(
            &self.boundary_schema_generation_hash,
            "boundary_schema_generation_hash",
        )?;
        for input in &self.index_inputs {
            ensure_algorithm_prefixed_hash(&input.definition_hash, "index_definition_hash")?;
        }
        Ok(())
    }
}

pub(super) fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
}
