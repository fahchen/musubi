//! Protocol tests over a scripted `MockSocket` plus a `ManualTimer`
//! (`docs/rust-client.md` §12, layer 3): join/rejoin, join failure, generation
//! guarding, heartbeat timeout, reply correlation and deliberate leaves.
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::time::Duration;

use phoenix_channel::{
    Channel, ChannelErrorReason, ChannelEvent, ChannelEvents, Message, PhoenixSocket, PushError,
    Reply, ReplyStatus,
};
use serde_json::{Value, json};

mod common;

use common::{Pump, Seams, Slot, drain, ended};

const TOPIC: &str = "musubi:connection:MyApp.CartStore:cart";
const HEARTBEAT: Duration = Duration::from_secs(30);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn joins_with_a_five_tuple_whose_ref_is_its_own_join_ref() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({"module": "MyApp.CartStore"}));

    channel.join().unwrap();
    harness.pump();

    assert_eq!(
        harness.connected_urls(),
        vec!["wss://example.test/socket/websocket?vsn=2.0.0&token=secret".to_owned()]
    );
    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { join_ref: Some(join_ref), msg_ref: Some(msg_ref), topic, event, payload }]
            if join_ref == msg_ref
                && join_ref == "1"
                && topic == TOPIC
                && event == "phx_join"
                && payload["module"] == json!("MyApp.CartStore")
    ));

    server.reply(
        &sent[0],
        ReplyStatus::Ok,
        json!({"root_id": "MyApp.CartStore:cart"}),
    );
    harness.pump();

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Joined { response }] if response["root_id"] == json!("MyApp.CartStore:cart")
    ));
}

#[test]
fn join_error_surfaces_the_server_reason_verbatim() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"reason": "unauthorized"}),
    );
    harness.pump();

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::JoinError { response }] if response["reason"] == json!("unauthorized")
    ));
}

#[test]
fn join_timeout_is_reported_as_a_join_failure() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    server.sent(&mut harness);
    harness.fire(JOIN_TIMEOUT);

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::JoinTimeout]
    ));
}

#[test]
fn a_join_reply_that_is_not_a_reply_payload_fails_the_join_instead_of_stranding_it() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);

    // No `status` key: the inflight entry is already removed by the time the
    // parse fails, so its own timeout can never fire on it.
    server.push(Message {
        join_ref: sent[0].join_ref.clone(),
        msg_ref: sent[0].msg_ref.clone(),
        topic: sent[0].topic.clone(),
        event: "phx_reply".to_owned(),
        payload: json!({"ok": true}),
    });
    harness.pump();

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::JoinTimeout]
    ));
}

#[test]
fn a_rejoin_after_reconnect_fires_join_ok_again() {
    let mut harness = Harness::new();
    let mut first = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({"id": "cart"}));

    channel.join().unwrap();
    harness.pump();
    let sent = first.sent(&mut harness);
    first.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Joined { .. }]
    ));

    // The transport dies; the channel stays registered so it is rejoined.
    let mut second = harness.queue_socket();
    first.disconnect();
    harness.pump();
    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Error {
            reason: ChannelErrorReason::SocketClosed
        }]
    ));

    harness.fire_backoff();
    let rejoin = second.sent(&mut harness);
    assert!(matches!(
        rejoin.as_slice(),
        [Message { event, payload, msg_ref: Some(msg_ref), .. }]
            if event == "phx_join" && payload["id"] == json!("cart") && msg_ref == "2"
    ));

    second.reply(&rejoin[0], ReplyStatus::Ok, json!({}));
    harness.pump();

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Joined { .. }]
    ));
}

#[test]
fn attaching_the_same_topic_again_invalidates_the_previous_handle() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (stale_channel, mut stale_events) = harness.attach(TOPIC, json!({}));

    stale_channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();

    let (fresh_channel, _fresh_events) = harness.attach(TOPIC, json!({}));

    assert!(fresh_channel.generation() > stale_channel.generation());
    assert!(ended(&mut stale_events));
    assert!(matches!(
        harness.push(&stale_channel, "command", json!({})),
        Err(PushError::Stale)
    ));
    // The superseded join reply must not resurrect the stale handle either.
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    assert!(ended(&mut stale_events));
}

#[test]
fn a_missed_heartbeat_reply_tears_the_socket_down() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    harness.fire(HEARTBEAT);
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { join_ref: None, topic, event, .. }]
            if topic == "phoenix" && event == "heartbeat"
    ));

    // No reply before the next tick: the socket is dead even though the
    // transport has not noticed.
    harness.fire(HEARTBEAT);

    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Error {
            reason: ChannelErrorReason::HeartbeatTimeout
        }]
    ));
}

#[test]
fn an_answered_heartbeat_keeps_the_socket_alive() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    harness.fire(HEARTBEAT);
    let beat = server.sent(&mut harness);
    server.reply(&beat[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    harness.fire(HEARTBEAT);

    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, .. }] if event == "heartbeat"
    ));
    assert!(drain(&mut events).is_empty());
}

#[test]
fn replies_are_correlated_by_ref_regardless_of_arrival_order() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, _events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();

    let first = harness.push_later(&channel, "command", json!({"name": "add"}));
    let second = harness.push_later(&channel, "command", json!({"name": "remove"}));
    harness.pump();

    let pushes = server.sent(&mut harness);
    assert!(matches!(
        pushes.as_slice(),
        [
            Message { msg_ref: Some(add), join_ref: Some(add_join), .. },
            Message { msg_ref: Some(remove), join_ref: Some(remove_join), .. },
        ] if add == "2" && remove == "3" && add_join == "1" && remove_join == "1"
    ));

    server.reply(&pushes[1], ReplyStatus::Error, json!({"reason": "nope"}));
    server.reply(&pushes[0], ReplyStatus::Ok, json!({"ok": true}));

    assert!(matches!(
        harness.settle(first),
        Ok(Reply { status: ReplyStatus::Ok, response }) if response["ok"] == json!(true)
    ));
    assert!(matches!(
        harness.settle(second),
        Ok(Reply { status: ReplyStatus::Error, response }) if response["reason"] == json!("nope")
    ));
}

#[test]
fn a_push_before_the_join_is_acknowledged_is_rejected() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, _events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    server.sent(&mut harness);

    assert!(matches!(
        harness.push(&channel, "command", json!({})),
        Err(PushError::NotJoined)
    ));
}

#[test]
fn a_push_times_out_when_no_reply_arrives() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, _events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();

    let pending = harness.push_later(&channel, "command", json!({}));
    harness.pump();
    server.sent(&mut harness);
    harness.fire(PUSH_TIMEOUT);

    assert!(matches!(harness.settle(pending), Err(PushError::Timeout)));
}

#[test]
fn a_deliberate_leave_suppresses_the_close_and_stops_rejoining() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    channel.leave().unwrap();
    harness.pump();
    let leave = server.sent(&mut harness);
    assert!(matches!(
        leave.as_slice(),
        [Message { event, join_ref: Some(join_ref), .. }]
            if event == "phx_leave" && join_ref == "1"
    ));

    server.reply(&leave[0], ReplyStatus::Ok, json!({}));
    server.push(Message {
        join_ref: None,
        msg_ref: None,
        topic: TOPIC.to_owned(),
        event: "phx_close".to_owned(),
        payload: json!({}),
    });
    harness.pump();

    assert!(ended(&mut events));

    // Nothing is scheduled to rejoin the topic we left.
    harness.fire_backoff();
    harness.fire(HEARTBEAT);
    let after_leave = server.sent(&mut harness);
    assert!(!after_leave.is_empty());
    assert!(
        after_leave
            .iter()
            .all(|message| message.event == "heartbeat")
    );
}

#[test]
fn a_close_arriving_before_the_leave_reply_is_suppressed() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    channel.leave().unwrap();
    harness.pump();
    server.sent(&mut harness);

    // The server tears the channel down without acknowledging the leave first.
    server.push(Message {
        join_ref: Some("1".to_owned()),
        msg_ref: None,
        topic: TOPIC.to_owned(),
        event: "phx_close".to_owned(),
        payload: json!({}),
    });
    harness.pump();

    assert!(ended(&mut events));

    harness.fire_backoff();
    assert!(
        server
            .sent(&mut harness)
            .iter()
            .all(|message| message.event != "phx_join")
    );
}

#[test]
fn a_server_close_reports_close_and_schedules_a_rejoin() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    server.push(Message {
        join_ref: Some("1".to_owned()),
        msg_ref: None,
        topic: TOPIC.to_owned(),
        event: "phx_close".to_owned(),
        payload: json!({}),
    });
    harness.pump();
    assert!(matches!(
        drain(&mut events).as_slice(),
        [ChannelEvent::Close]
    ));

    harness.fire_backoff();

    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, .. }] if event == "phx_join"
    ));
}

#[test]
fn server_messages_reach_the_channel_stream() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    server.push(Message {
        join_ref: Some("1".to_owned()),
        msg_ref: None,
        topic: TOPIC.to_owned(),
        event: "patch".to_owned(),
        payload: json!({"version": 1}),
    });
    harness.pump();

    assert!(matches!(
        drain(&mut events).as_slice(),
        [
            ChannelEvent::Joined { .. },
            ChannelEvent::Message { event, payload }
        ] if event == "patch" && payload["version"] == json!(1)
    ));
}

#[test]
fn disconnect_drops_every_channel_and_stops_reconnecting() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (channel, mut events) = harness.attach(TOPIC, json!({}));

    channel.join().unwrap();
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    harness.pump();
    drain(&mut events);

    harness.disconnect();

    assert!(ended(&mut events));
    harness.fire_backoff();
    assert!(server.sent(&mut harness).is_empty());
    assert!(matches!(
        harness.push(&channel, "command", json!({})),
        Err(PushError::Stale)
    ));
}

// ---------------------------------------------------------------- harness --

/// The shared rig (`tests/common/mod.rs`) wired to one [`PhoenixSocket`].
type Harness = common::Harness<PhoenixSocket>;

impl Harness {
    fn new() -> Self {
        Self::new_with(|seams: Seams| {
            PhoenixSocket::builder()
                .url("wss://example.test/socket")
                .param("token", "secret")
                .connector(seams.connector)
                .spawner(seams.spawner)
                .timer(seams.timer)
                .heartbeat(HEARTBEAT)
                .join_timeout(JOIN_TIMEOUT)
                .push_timeout(PUSH_TIMEOUT)
                .build()
                .expect("every seam is set")
        })
    }

    fn attach(&mut self, topic: &str, params: Value) -> (Channel, ChannelEvents) {
        let socket = self.inner.clone();
        let topic = topic.to_owned();
        let slot = self.spawn_capture(async move { socket.channel(topic, params).await });

        self.settle(slot).expect("the actor is running")
    }

    fn push(&mut self, channel: &Channel, event: &str, payload: Value) -> Result<Reply, PushError> {
        let pending = self.push_later(channel, event, payload);

        self.settle(pending)
    }

    fn push_later(
        &mut self,
        channel: &Channel,
        event: &str,
        payload: Value,
    ) -> Slot<Result<Reply, PushError>> {
        let channel = channel.clone();
        let event = event.to_owned();

        self.spawn_capture(async move { channel.push(event, payload).await })
    }

    fn disconnect(&mut self) {
        let socket = self.inner.clone();
        let slot = self.spawn_capture(async move { socket.disconnect().await });

        self.settle(slot).expect("the actor is running");
    }
}
