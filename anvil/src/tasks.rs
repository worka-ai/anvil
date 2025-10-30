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

#[derive(Debug, ToSql, FromSql, PartialEq, Eq)]
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
