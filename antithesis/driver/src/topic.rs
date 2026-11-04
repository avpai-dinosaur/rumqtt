//! MQTT topic-filter matching (spec section 4.7).
pub fn matches(filter: &str, topic: &str) -> bool {
    if filter == topic {
        return true;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(a), Some(b)) => {
                if a != b {
                    return false;
                }
            }
            (None, None) => return true,
            (Some(_), None) | (None, Some(_)) => return false,
        }
    }
}

pub fn is_wildcard(filter: &str) -> bool {
    filter.contains('+') || filter.contains('#')
}

#[cfg(test)]
mod tests {
    use super::matches;
    #[test]
    fn basics() {
        assert!(matches("t/a/x", "t/a/x"));
        assert!(matches("t/a/+", "t/a/x"));
        assert!(!matches("t/a/+", "t/a/x/y"));
        assert!(matches("t/#", "t/a/x"));
        assert!(matches("t/#", "t"));
        assert!(matches("#", "will/c1"));
        assert!(!matches("t/+/x", "t/a/y"));
        assert!(matches("t/+/x", "t/b/x"));
    }
}
