use super::*;

#[test]
fn authz_consistency_parses_latest_without_zookie() {
    assert_eq!(
        AuthzConsistency::from_request("", "").unwrap(),
        AuthzConsistency::Latest
    );
    assert_eq!(
        AuthzConsistency::from_request("latest", "").unwrap(),
        AuthzConsistency::Latest
    );
}

#[test]
fn authz_consistency_requires_zookie_for_at_least_and_exact() {
    assert_eq!(
        AuthzConsistency::from_request("at_least", "authz:42").unwrap(),
        AuthzConsistency::AtLeast(42)
    );
    assert_eq!(
        AuthzConsistency::from_request("exact", "authz:7").unwrap(),
        AuthzConsistency::Exact(7)
    );
    assert_eq!(
        AuthzConsistency::from_request("exact", "")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        AuthzConsistency::from_request("at_least", "authz:-1")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn authz_watch_cursor_split_round_trips() {
    let cursor = (u128::from(99_u64) << 64) | u128::from(42_u64);
    let (low, high) = split_u128(cursor);
    assert_eq!(join_u128(low, high), cursor);
}
