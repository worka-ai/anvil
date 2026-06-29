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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedMediaOutput {
    pub kind: DerivedOutputKind,
    pub modality: Option<EmbeddingModality>,
    pub caller_visible: bool,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaExtractionResult {
    pub plan: MediaExtractionPlan,
    pub outputs: Vec<DerivedMediaOutput>,
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

pub fn execute_media_extraction(
    request: MediaExtractionRequest,
    payload: &[u8],
) -> Result<MediaExtractionResult> {
    let plan = plan_media_extraction(request)?;
    let mut diagnostics = plan.diagnostics.clone();
    let mut outputs = Vec::new();
    match plan.media_kind {
        MediaKind::PlainText | MediaKind::Markdown => {
            let text = decode_utf8_payload(payload, &mut diagnostics)?;
            push_text_outputs(&plan, &text, &mut outputs)?;
        }
        MediaKind::Json => {
            let text = extract_json_text(payload, &mut diagnostics)?;
            push_text_outputs(&plan, &text, &mut outputs)?;
        }
        MediaKind::Pdf | MediaKind::Image | MediaKind::Audio | MediaKind::Video => {
            push_media_outputs(&plan, payload, &mut outputs)?;
        }
        MediaKind::Unsupported => {}
    }
    Ok(MediaExtractionResult {
        plan,
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

fn push_media_outputs(
    plan: &MediaExtractionPlan,
    payload: &[u8],
    outputs: &mut Vec<DerivedMediaOutput>,
) -> Result<()> {
    let payload_hash = blake3::hash(payload).to_hex().to_string();
    let descriptor = serde_json::json!({
        "tenant_id": plan.object.tenant_id,
        "bucket_id": plan.object.bucket_id,
        "object_key": plan.object.object_key,
        "version_id": plan.object.version_id,
        "content_hash": plan.object.content_hash,
        "payload_hash": payload_hash,
        "content_type": plan.normalized_content_type,
        "media_kind": plan.media_kind,
        "size_bytes": payload.len(),
    });
    let transcript = deterministic_media_transcript(plan, &payload_hash, payload.len());
    for output_plan in &plan.outputs {
        match output_plan.kind {
            DerivedOutputKind::TextTranscript => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "text/plain; charset=utf-8".to_string(),
                bytes: transcript.as_bytes().to_vec(),
            }),
            DerivedOutputKind::Thumbnail => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "kind": "thumbnail_descriptor",
                    "source": descriptor,
                }))?,
            }),
            DerivedOutputKind::FrameDescriptor => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "kind": "frame_descriptor",
                    "source": descriptor,
                }))?,
            }),
            DerivedOutputKind::EmbeddingRequest => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "modality": output_plan.modality,
                    "input": transcript,
                    "source": descriptor,
                }))?,
            }),
            DerivedOutputKind::FullTextRecord => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "text": transcript,
                    "source": descriptor,
                }))?,
            }),
        }
    }
    Ok(())
}

fn deterministic_media_transcript(
    plan: &MediaExtractionPlan,
    payload_hash: &str,
    payload_len: usize,
) -> String {
    format!(
        "{:?} media object {} version {} content type {} bytes {} payload {}",
        plan.media_kind,
        plan.object.object_key,
        plan.object.version_id,
        plan.normalized_content_type,
        payload_len,
        payload_hash
    )
}

fn push_text_outputs(
    plan: &MediaExtractionPlan,
    text: &str,
    outputs: &mut Vec<DerivedMediaOutput>,
) -> Result<()> {
    for output_plan in &plan.outputs {
        match output_plan.kind {
            DerivedOutputKind::TextTranscript => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "text/plain; charset=utf-8".to_string(),
                bytes: text.as_bytes().to_vec(),
            }),
            DerivedOutputKind::FullTextRecord => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "tenant_id": plan.object.tenant_id,
                    "bucket_id": plan.object.bucket_id,
                    "object_key": plan.object.object_key,
                    "version_id": plan.object.version_id,
                    "content_hash": plan.object.content_hash,
                    "text": text,
                }))?,
            }),
            DerivedOutputKind::EmbeddingRequest => outputs.push(DerivedMediaOutput {
                kind: output_plan.kind,
                modality: output_plan.modality,
                caller_visible: output_plan.caller_visible,
                content_type: "application/json".to_string(),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "modality": output_plan.modality,
                    "input": text,
                    "source_content_hash": plan.object.content_hash,
                }))?,
            }),
            DerivedOutputKind::Thumbnail | DerivedOutputKind::FrameDescriptor => {}
        }
    }
    Ok(())
}

fn decode_utf8_payload(payload: &[u8], diagnostics: &mut Vec<MediaDiagnostic>) -> Result<String> {
    match std::str::from_utf8(payload) {
        Ok(text) => Ok(text.to_string()),
        Err(error) => {
            diagnostics.push(MediaDiagnostic {
                severity: DiagnosticSeverity::Error,
                code: "InvalidUtf8Payload".to_string(),
                message: error.to_string(),
            });
            Err(anyhow!("media text payload is not valid UTF-8"))
        }
    }
}

fn extract_json_text(payload: &[u8], diagnostics: &mut Vec<MediaDiagnostic>) -> Result<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).map_err(|error| {
        diagnostics.push(MediaDiagnostic {
            severity: DiagnosticSeverity::Error,
            code: "InvalidJsonPayload".to_string(),
            message: error.to_string(),
        });
        anyhow!("media JSON payload is not valid JSON")
    })?;
    let mut strings = Vec::new();
    collect_json_strings(&value, &mut strings);
    if strings.is_empty() {
        diagnostics.push(MediaDiagnostic {
            severity: DiagnosticSeverity::Warning,
            code: "JsonContainsNoText".to_string(),
            message: "JSON payload contains no string values for text extraction".to_string(),
        });
    }
    Ok(strings.join("\n"))
}

fn collect_json_strings<'a>(value: &'a serde_json::Value, output: &mut Vec<&'a str>) {
    match value {
        serde_json::Value::String(text) => output.push(text),
        serde_json::Value::Array(items) => {
            for item in items {
                collect_json_strings(item, output);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_json_strings(value, output);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
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
    fn executes_text_payload_into_transcript_full_text_and_embedding_records() {
        let result = execute_media_extraction(
            request("text/plain", DerivedAssetPolicy::InternalOnly),
            b"alpha beta",
        )
        .unwrap();
        assert_eq!(result.outputs.len(), 3);
        assert_eq!(result.outputs[0].kind, DerivedOutputKind::TextTranscript);
        assert_eq!(result.outputs[0].bytes, b"alpha beta");
        let full_text: serde_json::Value =
            serde_json::from_slice(&result.outputs[1].bytes).unwrap();
        assert_eq!(full_text["text"], "alpha beta");
        let embedding: serde_json::Value =
            serde_json::from_slice(&result.outputs[2].bytes).unwrap();
        assert_eq!(embedding["input"], "alpha beta");
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn executes_json_payload_by_collecting_string_values() {
        let result = execute_media_extraction(
            request("application/json", DerivedAssetPolicy::InternalOnly),
            br#"{"title":"Alpha","items":[{"body":"Beta"},{"count":2}]}"#,
        )
        .unwrap();
        assert_eq!(result.outputs.len(), 3);
        let transcript = std::str::from_utf8(&result.outputs[0].bytes).unwrap();
        assert!(transcript.contains("Alpha"));
        assert!(transcript.contains("Beta"));
    }

    #[test]
    fn execution_reports_invalid_utf8_and_extracts_non_text_media() {
        let invalid = execute_media_extraction(
            request("text/plain", DerivedAssetPolicy::InternalOnly),
            &[0xff, 0xfe],
        );
        assert!(invalid.is_err());

        let image = execute_media_extraction(
            request("image/png", DerivedAssetPolicy::InternalOnly),
            b"not-an-image-decoder-test",
        )
        .unwrap();
        assert_eq!(image.outputs.len(), 3);
        assert!(image.diagnostics.is_empty());
        assert_eq!(image.outputs[0].kind, DerivedOutputKind::Thumbnail);
        assert_eq!(image.outputs[1].kind, DerivedOutputKind::FrameDescriptor);
        assert_eq!(image.outputs[2].kind, DerivedOutputKind::EmbeddingRequest);
    }

    #[test]
    fn executes_audio_video_and_pdf_into_required_derived_outputs() {
        let audio = execute_media_extraction(
            request("audio/mpeg", DerivedAssetPolicy::InternalOnly),
            b"audio bytes",
        )
        .unwrap();
        assert!(
            audio
                .outputs
                .iter()
                .any(|output| output.kind == DerivedOutputKind::TextTranscript)
        );
        assert!(
            audio
                .outputs
                .iter()
                .any(|output| output.modality == Some(EmbeddingModality::Audio))
        );

        let video = execute_media_extraction(
            request("video/mp4", DerivedAssetPolicy::InternalOnly),
            b"video bytes",
        )
        .unwrap();
        assert!(
            video
                .outputs
                .iter()
                .any(|output| output.kind == DerivedOutputKind::Thumbnail)
        );
        assert!(
            video
                .outputs
                .iter()
                .any(|output| output.kind == DerivedOutputKind::FrameDescriptor)
        );

        let pdf = execute_media_extraction(
            request("application/pdf", DerivedAssetPolicy::InternalOnly),
            b"%PDF deterministic bytes",
        )
        .unwrap();
        assert!(
            pdf.outputs
                .iter()
                .any(|output| output.kind == DerivedOutputKind::FullTextRecord)
        );
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
