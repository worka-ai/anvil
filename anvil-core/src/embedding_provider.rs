use crate::config::Config;
use crate::formats::vector::VectorIndexDefinition;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub const TEST_ONLY_EMBEDDING_PROVIDER: &str = "test_only";

#[derive(Debug, Clone)]
pub struct EmbeddingProviderRegistry {
    providers: Arc<BTreeMap<String, EmbeddingProviderDefinition>>,
    allow_test_only: bool,
}

#[derive(Debug, Clone)]
struct EmbeddingProviderDefinition {
    name: String,
    kind: EmbeddingProviderKind,
    timeout_ms: u64,
}

#[derive(Debug, Clone)]
enum EmbeddingProviderKind {
    CommandJson { command: String, args: Vec<String> },
}

#[derive(Debug, Deserialize)]
struct EmbeddingProviderFile {
    #[serde(default)]
    providers: Vec<EmbeddingProviderTomlOrJson>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingProviderTomlOrJson {
    name: String,
    kind: String,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingProviderRequest<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub model_version: Option<&'a str>,
    pub dimension: u16,
    pub modality: &'a str,
    pub normalisation: &'a str,
    pub chunking: &'a JsonValue,
    pub extractor: &'a JsonValue,
    pub input: EmbeddingProviderInput<'a>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EmbeddingProviderInput<'a> {
    Text { text: &'a str },
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddingProviderResponse {
    #[serde(default)]
    pub model_version: Option<String>,
    pub vectors: Vec<EmbeddingProviderVector>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddingProviderVector {
    pub values: Vec<f32>,
    #[serde(default)]
    pub chunk_id: Option<u32>,
    #[serde(default)]
    pub source_start: Option<u64>,
    #[serde(default)]
    pub source_len: Option<u32>,
}

impl EmbeddingProviderRegistry {
    pub fn from_config(config: &Config) -> Result<Self> {
        let providers = parse_provider_config(&config.vector_embedding_providers_json)?;
        Ok(Self {
            providers: Arc::new(providers),
            allow_test_only: config.allow_test_only_embedding_provider,
        })
    }

    pub fn for_tests(allow_test_only: bool) -> Self {
        Self {
            providers: Arc::new(BTreeMap::new()),
            allow_test_only,
        }
    }

    pub fn is_test_only_allowed(&self) -> bool {
        self.allow_test_only
    }

    pub fn has_provider(&self, name: &str) -> bool {
        self.providers.contains_key(name)
    }

    pub async fn embed_text(
        &self,
        definition: &VectorIndexDefinition,
        extractor: &JsonValue,
        payload: &[u8],
    ) -> Result<EmbeddingProviderResponse> {
        let text = std::str::from_utf8(payload)
            .context("vector provider text extractor requires UTF-8 payload")?;
        let provider = self
            .providers
            .get(&definition.embedding_provider)
            .ok_or_else(|| {
                anyhow!(
                    "embedding provider `{}` is not configured",
                    definition.embedding_provider
                )
            })?;
        let request = EmbeddingProviderRequest {
            provider: &definition.embedding_provider,
            model: &definition.embedding_model,
            model_version: definition.embedding_model_version.as_deref(),
            dimension: definition.dimension,
            modality: definition.modality.as_name(),
            normalisation: &definition.normalisation,
            chunking: &definition.chunking,
            extractor,
            input: EmbeddingProviderInput::Text { text },
        };
        provider.embed(request).await
    }
}

impl EmbeddingProviderDefinition {
    async fn embed(
        &self,
        request: EmbeddingProviderRequest<'_>,
    ) -> Result<EmbeddingProviderResponse> {
        match &self.kind {
            EmbeddingProviderKind::CommandJson { command, args } => {
                self.embed_with_command(command, args, request).await
            }
        }
    }

    async fn embed_with_command(
        &self,
        command: &str,
        args: &[String],
        request: EmbeddingProviderRequest<'_>,
    ) -> Result<EmbeddingProviderResponse> {
        let request_bytes = serde_json::to_vec(&request)?;
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn embedding provider `{}`", self.name))?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("embedding provider `{}` stdin unavailable", self.name))?;
        stdin.write_all(&request_bytes).await?;
        drop(stdin);

        let output = tokio::time::timeout(
            Duration::from_millis(self.timeout_ms.max(1)),
            child.wait_with_output(),
        )
        .await
        .with_context(|| format!("embedding provider `{}` timed out", self.name))??;

        if !output.status.success() {
            return Err(anyhow!(
                "embedding provider `{}` exited with status {}; stderr={}",
                self.name,
                output.status,
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(2048)
                    .collect::<String>()
            ));
        }
        serde_json::from_slice::<EmbeddingProviderResponse>(&output.stdout)
            .with_context(|| format!("decode embedding provider `{}` response", self.name))
    }
}

fn parse_provider_config(raw: &str) -> Result<BTreeMap<String, EmbeddingProviderDefinition>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(BTreeMap::new());
    }
    let parsed: EmbeddingProviderFile =
        serde_json::from_str(raw).context("parse vector embedding provider config")?;
    let mut providers = BTreeMap::new();
    for provider in parsed.providers {
        let name = provider.name.trim();
        if name.is_empty() {
            return Err(anyhow!("embedding provider name cannot be empty"));
        }
        if name == TEST_ONLY_EMBEDDING_PROVIDER {
            return Err(anyhow!(
                "test_only is reserved for the deterministic test provider"
            ));
        }
        if providers.contains_key(name) {
            return Err(anyhow!("duplicate embedding provider `{name}`"));
        }
        let timeout_ms = provider.timeout_ms.unwrap_or(30_000).max(1);
        let kind = match provider.kind.as_str() {
            "command_json" => {
                let command = provider
                    .command
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| anyhow!("command_json provider `{name}` requires command"))?;
                EmbeddingProviderKind::CommandJson {
                    command,
                    args: provider.args,
                }
            }
            other => return Err(anyhow!("unsupported embedding provider kind `{other}`")),
        };
        providers.insert(
            name.to_string(),
            EmbeddingProviderDefinition {
                name: name.to_string(),
                kind,
                timeout_ms,
            },
        );
    }
    Ok(providers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_config_rejects_reserved_and_duplicate_names() {
        assert!(
            parse_provider_config(
                r#"{"providers":[{"name":"test_only","kind":"command_json","command":"cat"}]}"#
            )
            .is_err()
        );
        assert!(parse_provider_config(r#"{"providers":[{"name":"prod","kind":"command_json","command":"cat"},{"name":"prod","kind":"command_json","command":"cat"}]}"#).is_err());
    }

    #[test]
    fn provider_config_parses_command_provider() {
        let providers = parse_provider_config(
            r#"{"providers":[{"name":"prod","kind":"command_json","command":"/bin/cat","args":["--help"],"timeout_ms":1000}]}"#,
        )
        .unwrap();
        assert!(providers.contains_key("prod"));
    }
}
