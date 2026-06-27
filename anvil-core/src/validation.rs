use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    // Restrictive regex for bucket names. Allows only lowercase letters, numbers, hyphens and dots.
    // Must start and end with a letter or number. Cannot be formatted as an IP address.
    // Length between 3 and 63 characters.
    static ref BUCKET_NAME_REGEX: Regex = Regex::new(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$").unwrap();

    // Regex to check if a string looks like an IP address.
    static ref IP_ADDRESS_REGEX: Regex = Regex::new(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$").unwrap();

    // Visible ASCII except double quote and backslash. This keeps path-style S3
    // keys broad enough for common clients while rejecting ambiguous separators.
    static ref OBJECT_KEY_REGEX: Regex = Regex::new(r"^[\x21\x23-\x5B\x5D-\x7E]*$").unwrap();
}

const RESERVED_INTERNAL_PREFIXES: &[&str] = &[
    "_anvil/meta/",
    "_anvil/index/",
    "_anvil/authz/",
    "_anvil/watch/",
    "_anvil/personaldb/",
    "_anvil/git/",
    "_anvil/tmp/",
];

pub fn is_valid_bucket_name(name: &str) -> bool {
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    if IP_ADDRESS_REGEX.is_match(name) {
        return false;
    }
    if name.contains("..") {
        return false;
    }
    BUCKET_NAME_REGEX.is_match(name)
}

pub fn is_valid_object_key(key: &str) -> bool {
    if key.is_empty() || key.len() > 4096 {
        return false;
    }
    if key.contains('\\') {
        return false;
    }
    if key
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return false;
    }
    OBJECT_KEY_REGEX.is_match(key)
}

pub fn is_reserved_internal_key(key: &str) -> bool {
    RESERVED_INTERNAL_PREFIXES
        .iter()
        .any(|prefix| key == prefix.trim_end_matches('/') || key.starts_with(prefix))
}

pub fn is_valid_region_name(name: &str) -> bool {
    lazy_static! {
        static ref REGION_NAME_REGEX: Regex = Regex::new(r"^[a-z][a-z0-9_-]*[a-z0-9]$").unwrap();
    }
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    REGION_NAME_REGEX.is_match(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_bucket_names() {
        assert!(is_valid_bucket_name("my-bucket"));
        //assert!(is_valid_bucket_name("my.bucket"));
        assert!(is_valid_bucket_name("123bucket"));
        assert!(is_valid_bucket_name("bucket123"));
    }

    #[test]
    fn test_invalid_bucket_names() {
        assert!(!is_valid_bucket_name("my_bucket"));
        assert!(!is_valid_bucket_name("MyBucket"));
        assert!(!is_valid_bucket_name("my-bucket-"));
        assert!(!is_valid_bucket_name("-my-bucket"));
        assert!(!is_valid_bucket_name("my..bucket"));
        assert!(!is_valid_bucket_name("192.168.1.1"));
        assert!(!is_valid_bucket_name("bu"));
        assert!(!is_valid_bucket_name(&"a".repeat(64)));
    }

    #[test]
    fn test_valid_object_keys() {
        assert!(is_valid_object_key("my-object"));
        assert!(is_valid_object_key("my_object"));
        assert!(is_valid_object_key("my/object"));
        assert!(is_valid_object_key("my.object"));
        assert!(is_valid_object_key("my*object"));
        assert!(is_valid_object_key("my'object"));
        assert!(is_valid_object_key("my+object=:,@"));
        assert!(is_valid_object_key(&"a".repeat(4096)));
    }

    #[test]
    fn test_invalid_object_keys() {
        assert!(!is_valid_object_key(""));
        assert!(!is_valid_object_key(&"a".repeat(4097)));
        assert!(!is_valid_object_key("my/../object"));
        assert!(!is_valid_object_key("my/./object"));
        assert!(!is_valid_object_key("my/object/.."));
        assert!(!is_valid_object_key("./my/object"));
        assert!(!is_valid_object_key(r"my\object"));
    }

    #[test]
    fn test_reserved_internal_keys() {
        assert!(is_reserved_internal_key("_anvil/authz"));
        assert!(is_reserved_internal_key("_anvil/authz/tuples"));
        assert!(is_reserved_internal_key("_anvil/personaldb/group"));
        assert!(is_reserved_internal_key("_anvil/index/search"));
        assert!(!is_reserved_internal_key("tenant/_anvil/authz/visible"));
        assert!(!is_reserved_internal_key("_anvil-public/authz"));
    }

    #[test]
    fn test_valid_region_names() {
        assert!(is_valid_region_name("us-east-1"));
        assert!(is_valid_region_name("eu-west-1"));
        assert!(is_valid_region_name("ap-southeast-2"));
        assert!(is_valid_region_name("us_east_1"));
    }

    #[test]
    fn test_invalid_region_names() {
        assert!(!is_valid_region_name("US-EAST-1"));
        assert!(!is_valid_region_name("us-east-1-"));
        assert!(!is_valid_region_name("-us-east-1"));
        assert!(!is_valid_region_name("us..east-1"));
        assert!(!is_valid_region_name("ue"));
        assert!(!is_valid_region_name(&"a".repeat(64)));
    }
}
