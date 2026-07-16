pub const DEFAULT_AUTHZ_REALM_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsersetSubjectRef<'a> {
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
}

pub fn encode_realm_namespace(realm_id: &str, namespace: &str) -> String {
    format!(
        "realm__{}__{namespace}",
        encode_realm_id_component(realm_id)
    )
}

fn encode_realm_id_component(realm_id: &str) -> String {
    let mut encoded = String::with_capacity(realm_id.len());
    for byte in realm_id.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' => encoded.push(byte as char),
            byte => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

pub fn encode_optional_realm_namespace(realm_id: &str, namespace: &str) -> String {
    if namespace.is_empty() {
        String::new()
    } else {
        encode_realm_namespace(realm_id, namespace)
    }
}

pub fn decode_realm_namespace<'a>(realm_id: &str, namespace: &'a str) -> Option<&'a str> {
    namespace.strip_prefix(&format!("realm__{}__", encode_realm_id_component(realm_id)))
}

pub fn split_realm_namespace(namespace: &str) -> Option<(String, &str)> {
    let rest = namespace.strip_prefix("realm__")?;
    let (encoded_realm_id, local_namespace) = rest.split_once("__")?;
    if local_namespace.is_empty() {
        return None;
    }
    decode_realm_id_component(encoded_realm_id).map(|realm_id| (realm_id, local_namespace))
}

fn decode_realm_id_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut idx = 0;
    let mut decoded = Vec::with_capacity(value.len());
    while idx < bytes.len() {
        match bytes[idx] {
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_nibble(bytes[idx + 1])?;
                let lo = hex_nibble(bytes[idx + 2])?;
                decoded.push(hi << 4 | lo);
                idx += 3;
            }
            b'/' => return None,
            byte => {
                decoded.push(byte);
                idx += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_realm_ids_are_safe_inside_userset_subjects() {
        let namespace = encode_realm_namespace("_anvil/system", "system");
        assert_eq!(
            decode_realm_namespace("_anvil/system", &namespace),
            Some("system")
        );
        let userset = format!("{namespace}/_anvil#manage_system");
        let parsed = parse_userset_subject(&userset).expect("encoded namespace has no slash");
        assert_eq!(parsed.namespace, namespace);
        assert_eq!(parsed.object_id, "_anvil");
        assert_eq!(parsed.relation, "manage_system");
        assert_eq!(
            split_realm_namespace(&namespace),
            Some(("_anvil/system".to_string(), "system"))
        );
    }

    #[test]
    fn realm_namespace_percent_encoding_round_trips_utf8_without_delimiter_injection() {
        let realm_id = "_tenant/emoji_\u{2615}";
        let namespace = encode_realm_namespace(realm_id, "bucket");
        assert!(!namespace.contains("emoji_"));
        assert_eq!(
            split_realm_namespace(&namespace),
            Some((realm_id.to_string(), "bucket"))
        );
    }
}
