//! Endpoint construction: `<base>/websocket?vsn=2.0.0&<connect params>`.

use std::collections::BTreeMap;

/// The serializer version this crate speaks.
pub(crate) const VSN: &str = "2.0.0";

/// Builds the WebSocket endpoint from the caller-supplied base URL.
///
/// Connect params are ordered by key so the URL is deterministic, and are the
/// only place credentials belong (join params are not authenticated).
///
pub(crate) fn endpoint_url(base: &str, params: &BTreeMap<String, String>) -> String {
    let mut url = format!("{}/websocket?vsn={VSN}", base.trim_end_matches('/'));

    for (key, value) in params {
        url.push('&');
        url.push_str(&percent_encode(key));
        url.push('=');
        url.push_str(&percent_encode(value));
    }

    url
}

/// Percent-encodes one query-string component (RFC 3986 unreserved set passes
/// through, everything else becomes `%XX`).
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_a_trailing_slash_and_encodes_reserved_characters() {
        let params = BTreeMap::from([("token".to_owned(), "a b/c".to_owned())]);

        assert_eq!(
            endpoint_url("wss://example.test/socket/", &params),
            "wss://example.test/socket/websocket?vsn=2.0.0&token=a%20b%2Fc"
        );
    }

    #[test]
    fn appends_websocket_and_vsn_without_params() {
        assert_eq!(
            endpoint_url("wss://example.test/socket", &BTreeMap::new()),
            "wss://example.test/socket/websocket?vsn=2.0.0"
        );
    }

    #[test]
    fn orders_params_by_key_and_encodes_them() {
        let params = BTreeMap::from([
            ("zed".to_owned(), "ü".to_owned()),
            ("api key".to_owned(), "1+2".to_owned()),
        ]);

        assert_eq!(
            endpoint_url("wss://example.test/socket", &params),
            "wss://example.test/socket/websocket?vsn=2.0.0&api%20key=1%2B2&zed=%C3%BC"
        );
    }
}
