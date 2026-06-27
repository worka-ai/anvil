#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnvilErrorCode {
    Unauthorized,
    UnauthorizedReservedNamespace,
    ForbiddenByPolicy,
    BucketNotFound,
    ObjectNotFound,
    VersionNotFound,
    PreconditionFailed,
    PartitionNotOwned,
    StaleFenceToken,
    ManifestInvalid,
    SegmentInvalid,
    IndexUnavailable,
    IndexDoesNotSupportQuery,
    AuthzRevisionUnavailable,
    WatchCursorExpired,
    PersonalDbGroupNotFound,
    PersonalDbStaleBase,
    PersonalDbInvalidChangeset,
    PersonalDbUnauthorizedMutation,
    PersonalDbProjectionWriteBackRejected,
    PersonalDbDivergentReplicaRequiresSnapshot,
    PersonalDbSnapshotInvalid,
    PayloadHashMismatch,
    UnsupportedFormatVersion,
}

impl AnvilErrorCode {
    pub const ALL: [Self; 24] = [
        Self::Unauthorized,
        Self::UnauthorizedReservedNamespace,
        Self::ForbiddenByPolicy,
        Self::BucketNotFound,
        Self::ObjectNotFound,
        Self::VersionNotFound,
        Self::PreconditionFailed,
        Self::PartitionNotOwned,
        Self::StaleFenceToken,
        Self::ManifestInvalid,
        Self::SegmentInvalid,
        Self::IndexUnavailable,
        Self::IndexDoesNotSupportQuery,
        Self::AuthzRevisionUnavailable,
        Self::WatchCursorExpired,
        Self::PersonalDbGroupNotFound,
        Self::PersonalDbStaleBase,
        Self::PersonalDbInvalidChangeset,
        Self::PersonalDbUnauthorizedMutation,
        Self::PersonalDbProjectionWriteBackRejected,
        Self::PersonalDbDivergentReplicaRequiresSnapshot,
        Self::PersonalDbSnapshotInvalid,
        Self::PayloadHashMismatch,
        Self::UnsupportedFormatVersion,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "Unauthorized",
            Self::UnauthorizedReservedNamespace => "UnauthorizedReservedNamespace",
            Self::ForbiddenByPolicy => "ForbiddenByPolicy",
            Self::BucketNotFound => "BucketNotFound",
            Self::ObjectNotFound => "ObjectNotFound",
            Self::VersionNotFound => "VersionNotFound",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::PartitionNotOwned => "PartitionNotOwned",
            Self::StaleFenceToken => "StaleFenceToken",
            Self::ManifestInvalid => "ManifestInvalid",
            Self::SegmentInvalid => "SegmentInvalid",
            Self::IndexUnavailable => "IndexUnavailable",
            Self::IndexDoesNotSupportQuery => "IndexDoesNotSupportQuery",
            Self::AuthzRevisionUnavailable => "AuthzRevisionUnavailable",
            Self::WatchCursorExpired => "WatchCursorExpired",
            Self::PersonalDbGroupNotFound => "PersonalDbGroupNotFound",
            Self::PersonalDbStaleBase => "PersonalDbStaleBase",
            Self::PersonalDbInvalidChangeset => "PersonalDbInvalidChangeset",
            Self::PersonalDbUnauthorizedMutation => "PersonalDbUnauthorizedMutation",
            Self::PersonalDbProjectionWriteBackRejected => "PersonalDbProjectionWriteBackRejected",
            Self::PersonalDbDivergentReplicaRequiresSnapshot => {
                "PersonalDbDivergentReplicaRequiresSnapshot"
            }
            Self::PersonalDbSnapshotInvalid => "PersonalDbSnapshotInvalid",
            Self::PayloadHashMismatch => "PayloadHashMismatch",
            Self::UnsupportedFormatVersion => "UnsupportedFormatVersion",
        }
    }
}

impl std::fmt::Display for AnvilErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AnvilErrorCode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|code| code.as_str() == value)
            .ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn error_code_strings_are_the_required_stable_contract() {
        let actual = AnvilErrorCode::ALL
            .into_iter()
            .map(AnvilErrorCode::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                "Unauthorized",
                "UnauthorizedReservedNamespace",
                "ForbiddenByPolicy",
                "BucketNotFound",
                "ObjectNotFound",
                "VersionNotFound",
                "PreconditionFailed",
                "PartitionNotOwned",
                "StaleFenceToken",
                "ManifestInvalid",
                "SegmentInvalid",
                "IndexUnavailable",
                "IndexDoesNotSupportQuery",
                "AuthzRevisionUnavailable",
                "WatchCursorExpired",
                "PersonalDbGroupNotFound",
                "PersonalDbStaleBase",
                "PersonalDbInvalidChangeset",
                "PersonalDbUnauthorizedMutation",
                "PersonalDbProjectionWriteBackRejected",
                "PersonalDbDivergentReplicaRequiresSnapshot",
                "PersonalDbSnapshotInvalid",
                "PayloadHashMismatch",
                "UnsupportedFormatVersion",
            ]
        );
    }

    #[test]
    fn error_code_parse_round_trips_every_code() {
        for code in AnvilErrorCode::ALL {
            assert_eq!(AnvilErrorCode::from_str(code.as_str()), Ok(code));
            assert_eq!(code.to_string(), code.as_str());
        }
        assert_eq!(AnvilErrorCode::from_str("NotAStableCode"), Err(()));
    }
}
