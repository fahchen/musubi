//! The client-owned RFC 6901 pointer walk (`docs/rust-reactive-state.md` §1.4).
//!
//! Only the addressing half lives here: token unescaping and the array index
//! rules. Applying an op is [`Transaction`](crate::Transaction)'s, against the
//! retained tree rather than against a `serde_json::Value`, and the op allowlist
//! stays where the envelope is decoded — so `move` / `copy` / `test` never
//! arrive.

use crate::error::TreeError;

/// Splits a pointer into its unescaped reference tokens.
///
/// `""` addresses the whole document and yields no tokens. Anything else must
/// start with `/`.
pub(crate) fn tokens(path: &str) -> Result<Vec<String>, TreeError> {
    if path.is_empty() {
        return Ok(Vec::new());
    }

    let Some(rest) = path.strip_prefix('/') else {
        return Err(TreeError::Pointer {
            path: path.to_owned(),
            reason: "a non-empty pointer must start with '/'",
        });
    };

    Ok(rest.split('/').map(unescape).collect())
}

/// Unescapes one reference token.
///
/// `~1` becomes `/` **before** `~0` becomes `~`; the other order would turn the
/// escaped `~01` back into `/` instead of `~1`.
fn unescape(token: &str) -> String {
    if !token.contains('~') {
        return token.to_owned();
    }

    let mut out = String::with_capacity(token.len());
    let mut chars = token.chars();

    while let Some(character) = chars.next() {
        if character != '~' {
            out.push(character);
            continue;
        }

        match chars.next() {
            Some('0') => out.push('~'),
            Some('1') => out.push('/'),
            // RFC 6901 leaves a stray `~` undefined; keeping it verbatim is
            // what every other implementation does, and the server never emits
            // one.
            Some(other) => {
                out.push('~');
                out.push(other);
            }
            None => out.push('~'),
        }
    }

    out
}

/// Where an array index token points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArrayIndex {
    /// A numeric token.
    At(usize),
    /// The `-` token: one past the end. Only `add` accepts it.
    End,
}

/// Reads an RFC 6901 array index token.
///
/// Leading zeros are rejected (`01` is not an index), as is anything that is
/// not `-` or a run of ASCII digits.
pub(crate) fn array_index(token: &str) -> Option<ArrayIndex> {
    if token == "-" {
        return Some(ArrayIndex::End);
    }

    if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    if token.len() > 1 && token.starts_with('0') {
        return None;
    }

    token.parse().ok().map(ArrayIndex::At)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_empty_pointer_addresses_the_whole_document() {
        assert_eq!(tokens("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_pointer_must_start_with_a_slash() {
        assert!(matches!(tokens("items/0"), Err(TreeError::Pointer { .. })));
    }

    #[test]
    fn a_trailing_slash_addresses_the_empty_key() {
        assert_eq!(tokens("/a/").unwrap(), ["a", ""]);
        assert_eq!(tokens("/").unwrap(), [""]);
    }

    #[test]
    fn tilde_one_is_unescaped_before_tilde_zero() {
        // `~01` must come back as `~1`, not as `/`.
        assert_eq!(tokens("/~01").unwrap(), ["~1"]);
        assert_eq!(tokens("/a~1b").unwrap(), ["a/b"]);
        assert_eq!(tokens("/c~0d").unwrap(), ["c~d"]);
        assert_eq!(tokens("/~0~1~0").unwrap(), ["~/~"]);
    }

    #[test]
    fn an_index_token_is_a_dash_or_digits_without_leading_zeros() {
        assert_eq!(array_index("-"), Some(ArrayIndex::End));
        assert_eq!(array_index("0"), Some(ArrayIndex::At(0)));
        assert_eq!(array_index("12"), Some(ArrayIndex::At(12)));
        assert_eq!(array_index("01"), None);
        assert_eq!(array_index(""), None);
        assert_eq!(array_index("1a"), None);
        assert_eq!(array_index("-1"), None);
        assert_eq!(array_index(" 1"), None);
    }
}
