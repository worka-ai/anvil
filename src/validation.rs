
use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
    // Restrictive regex for bucket names. Allows only lowercase letters, numbers, hyphens and dots.
    // Must start and end with a letter or number. Cannot be formatted as an IP address.
    // Length between 3 and 63 characters.
    static ref BUCKET_NAME_REGEX: Regex = Regex::new(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$").unwrap();

    // Regex to check if a string looks like an IP address.
    static ref IP_ADDRESS_REGEX: Regex = Regex::new(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$").unwrap();

    // Restrictive regex for object keys. Allows most valid characters, but disallows directory traversal and backslashes.
    static ref OBJECT_KEY_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9!-_.*'()/]*$").unwrap();
}

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
    if key.is_empty() || key.len() > 1024 {
        return false;
    }
    if key.contains("../") || key.contains("./") || key.contains('\\') {
        return false;
    }
    OBJECT_KEY_REGEX.is_match(key)
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
    }

    #[test]
    fn test_invalid_object_keys() {
        assert!(!is_valid_object_key(""));
        assert!(!is_valid_object_key(&"a".repeat(1025)));
        assert!(!is_valid_object_key("my/../object"));
        assert!(!is_valid_object_key("my/./object"));
        assert!(!is_valid_object_key(r"my\object"));
    }
}
