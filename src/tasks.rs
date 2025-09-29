use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "task_type")]
pub enum TaskType {
    #[postgres(name = "DELETE_OBJECT")]
    DeleteObject,
    #[postgres(name = "DELETE_BUCKET")]
    DeleteBucket,
    #[postgres(name = "REBALANCE_SHARD")]
    RebalanceShard,
}
