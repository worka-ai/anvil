pub const DEFAULT_AUTHZ_REALM_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsersetSubjectRef<'a> {
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
}

pub fn encode_realm_namespace(realm_id: &str, namespace: &str) -> String {
    format!("realm__{realm_id}__{namespace}")
}

pub fn encode_optional_realm_namespace(realm_id: &str, namespace: &str) -> String {
    if namespace.is_empty() {
        String::new()
    } else {
        encode_realm_namespace(realm_id, namespace)
    }
}

pub fn decode_realm_namespace<'a>(realm_id: &str, namespace: &'a str) -> Option<&'a str> {
    namespace.strip_prefix(&format!("realm__{realm_id}__"))
}

pub fn parse_userset_subject(value: &str) -> Option<UsersetSubjectRef<'_>> {
    let (namespace, rest) = value.split_once('/')?;
    let (object_id, relation) = rest.rsplit_once('#')?;
    if namespace.is_empty()
        || object_id.is_empty()
        || relation.is_empty()
        || namespace.chars().any(char::is_control)
        || object_id.chars().any(char::is_control)
        || relation.chars().any(char::is_control)
    {
        return None;
    }
    Some(UsersetSubjectRef {
        namespace,
        object_id,
        relation,
    })
}

pub fn encode_userset_subject_realm(
    realm_id: &str,
    subject_kind: &str,
    subject_id: &str,
) -> String {
    if subject_kind != "userset" {
        return subject_id.to_string();
    }
    let Some(subject) = parse_userset_subject(subject_id) else {
        return subject_id.to_string();
    };
    if decode_realm_namespace(realm_id, subject.namespace).is_some() {
        return subject_id.to_string();
    }
    format!(
        "{}/{}#{}",
        encode_realm_namespace(realm_id, subject.namespace),
        subject.object_id,
        subject.relation
    )
}

pub fn decode_userset_subject_realm(
    realm_id: &str,
    subject_kind: &str,
    subject_id: &str,
) -> String {
    if subject_kind != "userset" {
        return subject_id.to_string();
    }
    let Some(subject) = parse_userset_subject(subject_id) else {
        return subject_id.to_string();
    };
    let Some(namespace) = decode_realm_namespace(realm_id, subject.namespace) else {
        return subject_id.to_string();
    };
    format!("{}/{}#{}", namespace, subject.object_id, subject.relation)
}
