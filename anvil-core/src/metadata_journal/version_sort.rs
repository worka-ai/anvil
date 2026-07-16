use super::{ObjectVersionBody, helpers::parse_body_timestamp};

pub(super) fn object_versions_by_key(
    mut body_records: Vec<(usize, ObjectVersionBody)>,
) -> std::collections::BTreeMap<String, Vec<(usize, ObjectVersionBody)>> {
    body_records.sort_by_key(|(order, _)| *order);
    let mut versions_by_key =
        std::collections::BTreeMap::<String, Vec<(usize, ObjectVersionBody)>>::new();
    for (order, body) in body_records {
        let versions = versions_by_key.entry(body.object_key.clone()).or_default();
        if body.event == "delete_version" {
            versions.retain(|(_, existing)| existing.version_id != body.version_id);
        } else {
            versions.push((order, body));
        }
    }
    versions_by_key
}

pub(super) fn sort_versions_for_key(versions: &mut [(usize, ObjectVersionBody)]) {
    versions.sort_by(|(left_order, left), (right_order, right)| {
        parse_body_timestamp(&left.created_at)
            .ok()
            .cmp(&parse_body_timestamp(&right.created_at).ok())
            .then_with(|| left_order.cmp(right_order))
    });
}

pub(super) fn sort_versions_for_key_descending(versions: &mut [(usize, ObjectVersionBody)]) {
    versions.sort_by(|(left_order, left), (right_order, right)| {
        parse_body_timestamp(&right.created_at)
            .ok()
            .cmp(&parse_body_timestamp(&left.created_at).ok())
            .then_with(|| right_order.cmp(left_order))
    });
}
