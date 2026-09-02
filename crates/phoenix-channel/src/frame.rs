//! Serializer v2 framing: the five-tuple `[join_ref, ref, topic, event, payload]`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// One WebSocket frame as Phoenix sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A UTF-8 frame carrying one serializer v2 five-tuple.
    Text(String),
    /// A binary frame. Phoenix uses these for upload chunks only; this crate
    /// never emits one and logs-and-drops inbound ones.
    Binary(Vec<u8>),
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
    fn defaults_a_missing_reply_response() {
        let reply: Reply = serde_json::from_value(json!({"status": "ok"})).unwrap();

        assert!(reply.is_ok());
        assert_eq!(reply.response, Value::Null);
    }
}
