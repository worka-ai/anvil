use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedAssetPolicy {
    InternalOnly,
    PublishDerivedAssets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaKind {
    PlainText,
    Markdown,
    Json,
    Pdf,
    Image,
    Audio,
    Video,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedOutputKind {
    TextTranscript,
    Thumbnail,
    FrameDescriptor,
    EmbeddingRequest,
    FullTextRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingModality {
    Text,
    Image,
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaObjectRef {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub object_key: String,
    pub version_id: String,
    pub content_hash: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaExtractionRequest {
    pub object: MediaObjectRef,
    pub content_type: String,
    pub asset_policy: DerivedAssetPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedOutputPlan {
    pub kind: DerivedOutputKind,
    pub modality: Option<EmbeddingModality>,
    pub caller_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaDiagnostic {
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaExtractionPlan {
    pub object: MediaObjectRef,
    pub normalized_content_type: String,
    pub media_kind: MediaKind,
    pub outputs: Vec<DerivedOutputPlan>,
    pub diagnostics: Vec<MediaDiagnostic>,
}

impl MediaExtractionPlan {
    pub fn is_supported(&self) -> bool {
        self.media_kind != MediaKind::Unsupported
    }
}

pub fn plan_media_extraction(request: MediaExtractionRequest) -> Result<MediaExtractionPlan> {
    validate_request(&request)?;
    let normalized = normalize_content_type(&request.content_type);
    let media_kind = classify_media_kind(&normalized);
    let mut diagnostics = Vec::new();
    let outputs = match media_kind {
        MediaKind::PlainText | MediaKind::Markdown | MediaKind::Json => vec![
            output(
                DerivedOutputKind::TextTranscript,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::FullTextRecord,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::EmbeddingRequest,
                Some(EmbeddingModality::Text),
                request.asset_policy,
            ),
        ],
        MediaKind::Pdf => vec![
            output(
                DerivedOutputKind::TextTranscript,
                None,
                request.asset_policy,
            ),
            output(DerivedOutputKind::Thumbnail, None, request.asset_policy),
            output(
                DerivedOutputKind::FullTextRecord,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::EmbeddingRequest,
                Some(EmbeddingModality::Text),
                request.asset_policy,
            ),
        ],
        MediaKind::Image => vec![
            output(DerivedOutputKind::Thumbnail, None, request.asset_policy),
            output(
                DerivedOutputKind::FrameDescriptor,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::EmbeddingRequest,
                Some(EmbeddingModality::Image),
                request.asset_policy,
            ),
        ],
        MediaKind::Audio => vec![
            output(
                DerivedOutputKind::TextTranscript,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::EmbeddingRequest,
                Some(EmbeddingModality::Audio),
                request.asset_policy,
            ),
        ],
        MediaKind::Video => vec![
            output(
                DerivedOutputKind::TextTranscript,
                None,
                request.asset_policy,
            ),
            output(DerivedOutputKind::Thumbnail, None, request.asset_policy),
            output(
                DerivedOutputKind::FrameDescriptor,
                None,
                request.asset_policy,
            ),
            output(
                DerivedOutputKind::EmbeddingRequest,
                Some(EmbeddingModality::Video),
                request.asset_policy,
            ),
        ],
        MediaKind::Unsupported => {
            diagnostics.push(MediaDiagnostic {
                severity: DiagnosticSeverity::Warning,
                code: "UnsupportedMediaType".to_string(),
                message: format!(
                    "media extraction does not support content type `{}`",
                    normalized
                ),
            });
            Vec::new()
        }
    };
    Ok(MediaExtractionPlan {
        object: request.object,
        normalized_content_type: normalized,
        media_kind,
        outputs,
        diagnostics,
    })
}

pub fn classify_media_kind(content_type: &str) -> MediaKind {
    let normalized = normalize_content_type(content_type);
    match normalized.as_str() {
        "text/plain" => MediaKind::PlainText,
        "text/markdown" | "text/x-markdown" => MediaKind::Markdown,
        "application/json" | "text/json" => MediaKind::Json,
        "application/pdf" => MediaKind::Pdf,
        value if value.starts_with("image/") => MediaKind::Image,
        value if value.starts_with("audio/") => MediaKind::Audio,
        value if value.starts_with("video/") => MediaKind::Video,
        _ => MediaKind::Unsupported,
    }
}

fn output(
    kind: DerivedOutputKind,
    modality: Option<EmbeddingModality>,
    policy: DerivedAssetPolicy,
) -> DerivedOutputPlan {
    DerivedOutputPlan {
        kind,
        modality,
        caller_visible: policy == DerivedAssetPolicy::PublishDerivedAssets,
    }
}

fn normalize_content_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn validate_request(request: &MediaExtractionRequest) -> Result<()> {
    if request.object.tenant_id <= 0 {
        return Err(anyhow!("media object tenant id must be positive"));
    }
    if request.object.bucket_id <= 0 {
        return Err(anyhow!("media object bucket id must be positive"));
    }
    require_nonempty(&request.object.object_key, "object_key")?;
    require_nonempty(&request.object.version_id, "version_id")?;
    require_nonempty(&request.object.content_hash, "content_hash")?;
    require_nonempty(&request.content_type, "content_type")?;
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_minimum_supported_media_types() {
        assert_eq!(classify_media_kind("text/plain"), MediaKind::PlainText);
        assert_eq!(classify_media_kind("text/markdown"), MediaKind::Markdown);
        assert_eq!(classify_media_kind("application/json"), MediaKind::Json);
        assert_eq!(classify_media_kind("application/pdf"), MediaKind::Pdf);
        assert_eq!(classify_media_kind("image/png"), MediaKind::Image);
        assert_eq!(classify_media_kind("audio/mpeg"), MediaKind::Audio);
        assert_eq!(classify_media_kind("video/mp4"), MediaKind::Video);
        assert_eq!(
            classify_media_kind("application/octet-stream"),
            MediaKind::Unsupported
        );
    }

    #[test]
    fn plans_text_outputs_as_internal_derived_records_by_default() {
        let plan = plan_media_extraction(request(
            "Text/Plain; charset=utf-8",
            DerivedAssetPolicy::InternalOnly,
        ))
        .unwrap();
        assert_eq!(plan.normalized_content_type, "text/plain");
        assert_eq!(plan.media_kind, MediaKind::PlainText);
        assert_eq!(
            kinds(&plan),
            vec![
                DerivedOutputKind::TextTranscript,
                DerivedOutputKind::FullTextRecord,
                DerivedOutputKind::EmbeddingRequest,
            ]
        );
        assert!(plan.outputs.iter().all(|output| !output.caller_visible));
        assert!(plan.diagnostics.is_empty());
    }

    #[test]
    fn plans_media_outputs_by_modality() {
        let image =
            plan_media_extraction(request("image/webp", DerivedAssetPolicy::InternalOnly)).unwrap();
        assert_eq!(
            kinds(&image),
            vec![
                DerivedOutputKind::Thumbnail,
                DerivedOutputKind::FrameDescriptor,
                DerivedOutputKind::EmbeddingRequest,
            ]
        );
        assert_eq!(
            image.outputs.last().and_then(|output| output.modality),
            Some(EmbeddingModality::Image)
        );

        let video =
            plan_media_extraction(request("video/mp4", DerivedAssetPolicy::InternalOnly)).unwrap();
        assert_eq!(
            kinds(&video),
            vec![
                DerivedOutputKind::TextTranscript,
                DerivedOutputKind::Thumbnail,
                DerivedOutputKind::FrameDescriptor,
                DerivedOutputKind::EmbeddingRequest,
            ]
        );
        assert_eq!(
            video.outputs.last().and_then(|output| output.modality),
            Some(EmbeddingModality::Video)
        );
    }

    #[test]
    fn explicit_policy_allows_derived_assets_to_be_published() {
        let plan = plan_media_extraction(request(
            "application/pdf",
            DerivedAssetPolicy::PublishDerivedAssets,
        ))
        .unwrap();
        assert!(plan.outputs.iter().all(|output| output.caller_visible));
    }

    #[test]
    fn unsupported_formats_create_diagnostics_without_outputs() {
        let plan = plan_media_extraction(request(
            "application/x-custom",
            DerivedAssetPolicy::PublishDerivedAssets,
        ))
        .unwrap();
        assert_eq!(plan.media_kind, MediaKind::Unsupported);
        assert!(plan.outputs.is_empty());
        assert_eq!(plan.diagnostics.len(), 1);
        assert_eq!(plan.diagnostics[0].code, "UnsupportedMediaType");
    }

    #[test]
    fn rejects_incomplete_object_references() {
        let mut invalid = request("text/plain", DerivedAssetPolicy::InternalOnly);
        invalid.object.version_id.clear();
        assert!(plan_media_extraction(invalid).is_err());
    }

    fn request(content_type: &str, asset_policy: DerivedAssetPolicy) -> MediaExtractionRequest {
        MediaExtractionRequest {
            object: MediaObjectRef {
                tenant_id: 1,
                bucket_id: 2,
                object_key: "docs/a.txt".to_string(),
                version_id: uuid::Uuid::new_v4().to_string(),
                content_hash: hex::encode([7; 32]),
                size_bytes: 128,
            },
            content_type: content_type.to_string(),
            asset_policy,
        }
    }

    fn kinds(plan: &MediaExtractionPlan) -> Vec<DerivedOutputKind> {
        plan.outputs.iter().map(|output| output.kind).collect()
    }
}
