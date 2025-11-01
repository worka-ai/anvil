use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql, PartialEq, Eq)]
#[postgres(name = "task_type")]
pub enum TaskType {
    #[postgres(name = "DELETE_OBJECT")]
    DeleteObject,
    #[postgres(name = "DELETE_BUCKET")]
    DeleteBucket,
    #[postgres(name = "REBALANCE_SHARD")]
    RebalanceShard,
    #[postgres(name = "HF_INGESTION")]
    HFIngestion,
}

#[derive(Debug, ToSql, FromSql, PartialEq, Eq, Clone, Copy)]
#[postgres(name = "task_status")]
pub enum TaskStatus {
    #[postgres(name = "pending")]
    Pending,
    #[postgres(name = "running")]
    Running,
    #[postgres(name = "completed")]
    Completed,
    #[postgres(name = "failed")]
    Failed,
}

#[derive(Debug, ToSql, FromSql, PartialEq, Eq, Clone, Copy)]
#[postgres(name = "hf_ingestion_state")]
pub enum HFIngestionState {
    #[postgres(name = "queued")]
    Queued,
    #[postgres(name = "running")]
    Running,
    #[postgres(name = "completed")]
    Completed,
    #[postgres(name = "failed")]
    Failed,
    #[postgres(name = "canceled")]
    Canceled,
}

#[derive(Debug, ToSql, FromSql, PartialEq, Eq, Clone, Copy)]
#[postgres(name = "hf_item_state")]
pub enum HFIngestionItemState {
    #[postgres(name = "queued")]
    Queued,
    #[postgres(name = "downloading")]
    Downloading,
    #[postgres(name = "stored")]
    Stored,
    #[postgres(name = "failed")]
    Failed,
    #[postgres(name = "skipped")]
    Skipped,
}
