use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchResumeInput {
    pub watch_stream_id: String,
    pub consumer_id: String,
    pub checkpoint_cursor: u128,
    pub retained_start_cursor: u128,
    pub latest_cursor: u128,
    pub manifest_checkpoint_cursor: u128,
    pub source_manifest_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchResumePlan {
    UpToDate {
        cursor: u128,
    },
    Replay {
        from_exclusive: u128,
        to_inclusive: u128,
    },
    RebuildFromManifest {
        manifest_checkpoint_cursor: u128,
        source_manifest_hash: String,
        resume_after_cursor: u128,
    },
}

impl WatchResumePlan {
    pub fn resume_cursor(&self) -> u128 {
        match self {
            Self::UpToDate { cursor } => *cursor,
            Self::Replay { to_inclusive, .. } => *to_inclusive,
            Self::RebuildFromManifest {
                resume_after_cursor,
                ..
            } => *resume_after_cursor,
        }
    }
}

pub fn plan_watch_resume(input: WatchResumeInput) -> Result<WatchResumePlan> {
    validate_input(&input)?;
    if input.checkpoint_cursor > input.latest_cursor {
        return Err(anyhow!("watch checkpoint cursor is ahead of latest cursor"));
    }
    if input.checkpoint_cursor == input.latest_cursor {
        return Ok(WatchResumePlan::UpToDate {
            cursor: input.latest_cursor,
        });
    }
    if input.checkpoint_cursor < input.retained_start_cursor {
        return Ok(WatchResumePlan::RebuildFromManifest {
            manifest_checkpoint_cursor: input.manifest_checkpoint_cursor,
            source_manifest_hash: input.source_manifest_hash,
            resume_after_cursor: input.manifest_checkpoint_cursor,
        });
    }
    Ok(WatchResumePlan::Replay {
        from_exclusive: input.checkpoint_cursor,
        to_inclusive: input.latest_cursor,
    })
}

fn validate_input(input: &WatchResumeInput) -> Result<()> {
    require_nonempty(&input.watch_stream_id, "watch_stream_id")?;
    require_nonempty(&input.consumer_id, "consumer_id")?;
    validate_hex32(&input.source_manifest_hash, "source_manifest_hash")?;
    if input.retained_start_cursor > input.latest_cursor {
        return Err(anyhow!(
            "watch retained start cursor is after latest cursor"
        ));
    }
    if input.manifest_checkpoint_cursor > input.latest_cursor {
        return Err(anyhow!(
            "watch manifest checkpoint cursor is after latest cursor"
        ));
    }
    if input.manifest_checkpoint_cursor < input.retained_start_cursor {
        return Err(anyhow!(
            "watch manifest checkpoint cursor is outside retained window"
        ));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
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
    fn watch_resume_replays_when_checkpoint_is_inside_retention_window() {
        assert_eq!(
            plan_watch_resume(input(50, 40, 100, 40)).unwrap(),
            WatchResumePlan::Replay {
                from_exclusive: 50,
                to_inclusive: 100,
            }
        );
    }

    #[test]
    fn watch_resume_reports_up_to_date_when_checkpoint_matches_latest() {
        assert_eq!(
            plan_watch_resume(input(100, 40, 100, 40)).unwrap(),
            WatchResumePlan::UpToDate { cursor: 100 }
        );
    }

    #[test]
    fn watch_resume_rebuilds_from_manifest_when_checkpoint_expired() {
        assert_eq!(
            plan_watch_resume(input(39, 40, 100, 60)).unwrap(),
            WatchResumePlan::RebuildFromManifest {
                manifest_checkpoint_cursor: 60,
                source_manifest_hash: hex::encode([9; 32]),
                resume_after_cursor: 60,
            }
        );
    }

    #[test]
    fn watch_resume_rejects_inconsistent_cursor_windows() {
        assert!(plan_watch_resume(input(101, 40, 100, 40)).is_err());
        assert!(plan_watch_resume(input(50, 110, 100, 110)).is_err());
        assert!(plan_watch_resume(input(50, 40, 100, 101)).is_err());
        assert!(plan_watch_resume(input(50, 40, 100, 39)).is_err());
        let mut invalid = input(50, 40, 100, 40);
        invalid.source_manifest_hash = "not-hex".to_string();
        assert!(plan_watch_resume(invalid).is_err());
    }

    fn input(
        checkpoint_cursor: u128,
        retained_start_cursor: u128,
        latest_cursor: u128,
        manifest_checkpoint_cursor: u128,
    ) -> WatchResumeInput {
        WatchResumeInput {
            watch_stream_id: "object-prefix".to_string(),
            consumer_id: "index-builder".to_string(),
            checkpoint_cursor,
            retained_start_cursor,
            latest_cursor,
            manifest_checkpoint_cursor,
            source_manifest_hash: hex::encode([9; 32]),
        }
    }
}
