//! Serializer v2 framing: the five-tuple `[join_ref, ref, topic, event, payload]`
//! for JSON payloads, and the length-prefixed binary framing for raw ones.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// The `phx_join` lifecycle event.
pub(crate) const EVENT_JOIN: &str = "phx_join";
/// The `phx_leave` lifecycle event.
pub(crate) const EVENT_LEAVE: &str = "phx_leave";
/// The `phx_close` lifecycle event.
pub(crate) const EVENT_CLOSE: &str = "phx_close";
/// The `phx_error` lifecycle event.
pub(crate) const EVENT_ERROR: &str = "phx_error";
/// The event every reply is delivered under.
pub(crate) const EVENT_REPLY: &str = "phx_reply";
/// The heartbeat event.
pub(crate) const EVENT_HEARTBEAT: &str = "heartbeat";
/// The topic heartbeats are pushed on.
pub(crate) const TOPIC_PHOENIX: &str = "phoenix";

/// The serializer v2 binary-framing kind byte of a client→server push.
const BINARY_PUSH_KIND: u8 = 0;
/// The kind byte plus the four length bytes.
const BINARY_HEADER_LEN: usize = 5;

/// One WebSocket frame as Phoenix sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A UTF-8 frame carrying one serializer v2 five-tuple.
    Text(String),
    /// A binary frame, carrying a raw payload under a length-prefixed header
    /// ([`BinaryPush`]). Phoenix uses these for upload chunks only; inbound
    /// ones are logged and dropped, because a Musubi server never sends one —
    /// even a chunk's reply comes back as an ordinary text `phx_reply`.
    Binary(Vec<u8>),
}

/// A client→server push whose payload is raw bytes rather than JSON.
///
/// Phoenix frames these itself rather than through the five-tuple: a byte for
/// the kind, four length bytes, the four header strings, then the payload
/// verbatim (`Phoenix.Socket.V2.JSONSerializer.decode_binary/1`). `join_ref`
/// and `msg_ref` are `""` when absent, matching the server's `to_string(nil)`.
///
/// Only this one direction is modelled. The three server→client binary layouts
/// (push, reply, broadcast) have different headers and Musubi never sends one,
/// so [`decode`](Self::decode) reads back exactly what
/// [`encode`](Self::encode) writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryPush {
    /// The ref of the `phx_join` that established the sending channel.
    pub join_ref: String,
    /// The ref of this push, echoed back on its reply.
    pub msg_ref: String,
    /// The channel topic.
    pub topic: String,
    /// The event name.
    pub event: String,
    /// The payload, byte for byte.
    pub payload: Vec<u8>,
}

impl BinaryPush {
    /// Encodes the push as a binary [`Frame`].
    ///
    /// ```
    /// use phoenix_channel::{BinaryPush, Frame};
    ///
    /// let push = BinaryPush {
    ///     join_ref: "1".to_owned(),
    ///     msg_ref: "2".to_owned(),
    ///     topic: "musubi_upload:u_a3f".to_owned(),
    ///     event: "chunk".to_owned(),
    ///     payload: vec![0xde, 0xad],
    /// };
    ///
    /// let Frame::Binary(bytes) = push.encode().unwrap() else {
    ///     panic!("a binary push encodes to a binary frame")
    /// };
    ///
    /// assert_eq!(bytes[0], 0);
    /// assert_eq!(&bytes[bytes.len() - 2..], &[0xde, 0xad]);
    /// ```
    pub fn encode(&self) -> Result<Frame, BinaryFrameError> {
        let fields = [
            ("join_ref", &self.join_ref),
            ("ref", &self.msg_ref),
            ("topic", &self.topic),
            ("event", &self.event),
        ];

        let mut bytes = Vec::with_capacity(BINARY_HEADER_LEN + self.payload.len());
        bytes.push(BINARY_PUSH_KIND);

        for (name, value) in fields {
            let len = value.len();

            // The header length is one byte per field, so an oversized field
            // is unrepresentable rather than merely rejected by the server.
            let len = u8::try_from(len)
                .map_err(|_| BinaryFrameError::FieldTooLong { field: name, len })?;

            bytes.push(len);
        }

        for (_, value) in fields {
            bytes.extend_from_slice(value.as_bytes());
        }

        bytes.extend_from_slice(&self.payload);

        Ok(Frame::Binary(bytes))
    }

    /// Decodes what [`encode`](Self::encode) produced.
    ///
    /// ```
    /// use phoenix_channel::{BinaryPush, Frame};
    ///
    /// let push = BinaryPush {
    ///     join_ref: "1".to_owned(),
    ///     msg_ref: "2".to_owned(),
    ///     topic: "musubi_upload:u_a3f".to_owned(),
    ///     event: "chunk".to_owned(),
    ///     payload: b"hello".to_vec(),
    /// };
    ///
    /// let Frame::Binary(bytes) = push.encode().unwrap() else { unreachable!() };
    ///
    /// assert_eq!(BinaryPush::decode(&bytes).unwrap(), push);
    /// ```
    pub fn decode(bytes: &[u8]) -> Result<Self, BinaryFrameError> {
        let header = bytes
            .get(..BINARY_HEADER_LEN)
            .ok_or(BinaryFrameError::Truncated)?;

        if header[0] != BINARY_PUSH_KIND {
            return Err(BinaryFrameError::UnsupportedKind { kind: header[0] });
        }

        let names = ["join_ref", "ref", "topic", "event"];
        let mut parts = [const { String::new() }; 4];
        let mut rest = &bytes[BINARY_HEADER_LEN..];

        for (index, part) in parts.iter_mut().enumerate() {
            let len = usize::from(header[index + 1]);
            let (field, tail) = rest
                .split_at_checked(len)
                .ok_or(BinaryFrameError::Truncated)?;

            rest = tail;
            *part = std::str::from_utf8(field)
                .map_err(|_| BinaryFrameError::InvalidUtf8 {
                    field: names[index],
                })?
                .to_owned();
        }

        let [join_ref, msg_ref, topic, event] = parts;

        Ok(Self {
            join_ref,
            msg_ref,
            topic,
            event,
            payload: rest.to_vec(),
        })
    }
}

/// Why a binary frame could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum BinaryFrameError {
    /// A header field was longer than the single length byte can describe.
    #[error("binary frame {field} is {len} bytes; the header allows at most 255")]
    FieldTooLong {
        /// Which header field: `join_ref`, `ref`, `topic` or `event`.
        field: &'static str,
        /// Its actual length in bytes.
        len: usize,
    },
    /// The frame ended inside its header or one of its header fields.
    #[error("binary frame is truncated")]
    Truncated,
    /// The kind byte was not a client→server push; see [`BinaryPush`].
    #[error("binary frame kind {kind} is not a client push")]
    UnsupportedKind {
        /// The kind byte, verbatim.
        kind: u8,
    },
    /// A header field was not UTF-8.
    #[error("binary frame {field} is not utf-8")]
    InvalidUtf8 {
        /// Which header field.
        field: &'static str,
    },
}

/// A decoded serializer v2 message.
///
/// `join_ref` and `msg_ref` are nullable strings on the wire. Server-initiated
/// messages carry neither; replies echo the ref of the push that caused them.
///
/// Deliberately not `Serialize`/`Deserialize`: the wire form is the serializer
/// v2 five-tuple built by [`Message::encode`], not this struct's field names.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// The ref of the `phx_join` that established the sending channel.
    pub join_ref: Option<String>,
    /// The ref of this message, echoed back on its reply.
    pub msg_ref: Option<String>,
    /// The channel topic, or `"phoenix"` for heartbeats.
    pub topic: String,
    /// The event name.
    pub event: String,
    /// The event payload; always a JSON value, usually an object.
    pub payload: Value,
}

impl Message {
    /// Encodes the message as a text [`Frame`].
    ///
    /// ```
    /// use phoenix_channel::{Frame, Message};
    /// use serde_json::json;
    ///
    /// let message = Message {
    ///     join_ref: Some("1".to_owned()),
    ///     msg_ref: Some("2".to_owned()),
    ///     topic: "musubi:connection:Store:cart".to_owned(),
    ///     event: "command".to_owned(),
    ///     payload: json!({"name": "add"}),
    /// };
    ///
    /// assert_eq!(
    ///     message.encode(),
    ///     Frame::Text(
    ///         r#"["1","2","musubi:connection:Store:cart","command",{"name":"add"}]"#.to_owned()
    ///     )
    /// );
    /// ```
    pub fn encode(&self) -> Frame {
        let tuple = (
            &self.join_ref,
            &self.msg_ref,
            &self.topic,
            &self.event,
            &self.payload,
        );

        // Serializing a tuple of owned-JSON-safe parts cannot fail.
        Frame::Text(serde_json::to_string(&tuple).expect("five-tuple is serializable"))
    }

    /// Decodes a serializer v2 five-tuple.
    ///
    /// ```
    /// use phoenix_channel::Message;
    ///
    /// let message = Message::decode(r#"[null,null,"room","new_msg",{"body":"hi"}]"#).unwrap();
    ///
    /// assert_eq!(message.join_ref, None);
    /// assert_eq!(message.event, "new_msg");
    /// ```
    pub fn decode(text: &str) -> Result<Self, serde_json::Error> {
        let (join_ref, msg_ref, topic, event, payload) =
            serde_json::from_str::<(Option<String>, Option<String>, String, String, Value)>(text)?;

        Ok(Self {
            join_ref,
            msg_ref,
            topic,
            event,
            payload,
        })
    }
}

/// The status of a `phx_reply`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplyStatus {
    /// The push succeeded.
    Ok,
    /// The push was rejected; `response` carries the server's reason.
    Error,
}

/// The payload of a `phx_reply`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    /// Whether the originating push succeeded.
    pub status: ReplyStatus,
    /// The server-authored response body, verbatim.
    #[serde(default)]
    pub response: Value,
}

impl Reply {
    /// Returns `true` when the push succeeded.
    ///
    /// ```
    /// use phoenix_channel::{Reply, ReplyStatus};
    /// use serde_json::json;
    ///
    /// let reply = Reply { status: ReplyStatus::Ok, response: json!({}) };
    /// assert!(reply.is_ok());
    /// ```
    pub fn is_ok(&self) -> bool {
        matches!(self.status, ReplyStatus::Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encodes_and_decodes_round_trip() {
        let message = Message {
            join_ref: Some("7".to_owned()),
            msg_ref: Some("8".to_owned()),
            topic: "musubi:connection:Cart:1".to_owned(),
            event: EVENT_JOIN.to_owned(),
            payload: json!({"module": "Cart", "id": "1", "params": {}}),
        };

        let Frame::Text(text) = message.encode() else {
            panic!("encode must produce a text frame")
        };

        assert_eq!(Message::decode(&text).unwrap(), message);
    }

    #[test]
    fn decodes_null_refs() {
        let message = Message::decode(r#"[null,null,"phoenix","phx_error",{}]"#).unwrap();

        assert!(matches!(
            message,
            Message {
                join_ref: None,
                msg_ref: None,
                ..
            }
        ));
    }

    #[test]
    fn rejects_non_tuple_frames() {
        assert!(Message::decode(r#"{"topic":"room"}"#).is_err());
        assert!(Message::decode(r#"[null,null,"room","evt"]"#).is_err());
    }

    #[test]
    fn decodes_reply_payloads() {
        let reply: Reply = serde_json::from_value(json!({
            "status": "error",
            "response": {"reason": "unauthorized"}
        }))
        .unwrap();

        assert!(matches!(
            reply,
            Reply {
                status: ReplyStatus::Error,
                ..
            }
        ));
        assert_eq!(reply.response["reason"], json!("unauthorized"));
    }

    #[test]
    fn encodes_a_binary_push_with_the_serializer_v2_header() {
        let push = BinaryPush {
            join_ref: "1".to_owned(),
            msg_ref: "23".to_owned(),
            topic: "musubi_upload:u_a3f".to_owned(),
            event: "chunk".to_owned(),
            payload: vec![1, 2, 3],
        };

        let Frame::Binary(bytes) = push.encode().unwrap() else {
            panic!("a binary push encodes to a binary frame")
        };

        assert_eq!(&bytes[..5], &[0, 1, 2, 19, 5], "kind then four lengths");
        assert_eq!(&bytes[5..5 + 1 + 2], b"123", "join_ref then ref");
        assert_eq!(&bytes[bytes.len() - 3..], &[1, 2, 3], "payload verbatim");
        assert_eq!(BinaryPush::decode(&bytes).unwrap(), push);
    }

    #[test]
    fn rejects_a_header_field_that_does_not_fit_its_length_byte() {
        let push = BinaryPush {
            join_ref: String::new(),
            msg_ref: String::new(),
            topic: "t".repeat(256),
            event: "chunk".to_owned(),
            payload: Vec::new(),
        };

        assert!(matches!(
            push.encode(),
            Err(BinaryFrameError::FieldTooLong { field, len }) if field == "topic" && len == 256
        ));
    }

    #[test]
    fn rejects_binary_frames_it_did_not_write() {
        assert!(matches!(
            BinaryPush::decode(&[0, 1, 2]),
            Err(BinaryFrameError::Truncated)
        ));
        assert!(
            matches!(
                BinaryPush::decode(&[0, 4, 0, 0, 0, b'x']),
                Err(BinaryFrameError::Truncated)
            ),
            "a header field that runs past the end of the frame"
        );
        assert!(matches!(
            BinaryPush::decode(&[1, 0, 0, 0, 0]),
            Err(BinaryFrameError::UnsupportedKind { kind: 1 })
        ));
        assert!(matches!(
            BinaryPush::decode(&[0, 1, 0, 0, 0, 0xff]),
            Err(BinaryFrameError::InvalidUtf8 { field }) if field == "join_ref"
        ));
    }

    #[test]
    fn defaults_a_missing_reply_response() {
        let reply: Reply = serde_json::from_value(json!({"status": "ok"})).unwrap();

        assert!(reply.is_ok());
        assert_eq!(reply.response, Value::Null);
    }
}
