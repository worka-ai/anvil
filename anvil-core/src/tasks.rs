#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskType {
    DeleteObject,
    DeleteBucket,
    ObjectMetadataCompaction,
    IndexBuild,
    RebalanceShard,
    HFIngestion,
    AuthzMaterialization,
}

impl TaskType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeleteObject => "DELETE_OBJECT",
            Self::DeleteBucket => "DELETE_BUCKET",
            Self::ObjectMetadataCompaction => "OBJECT_METADATA_COMPACTION",
            Self::IndexBuild => "INDEX_BUILD",
            Self::RebalanceShard => "REBALANCE_SHARD",
            Self::HFIngestion => "HF_INGESTION",
            Self::AuthzMaterialization => "AUTHZ_MATERIALIZATION",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HFIngestionState {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

impl HFIngestionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HFIngestionItemState {
    Queued,
    Downloading,
    Stored,
    Failed,
    Skipped,
}
