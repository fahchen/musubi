@runtime @transport @connection-root
Feature: Connection Root Identity
  As a connected client mounting one or more root stores on a single Musubi socket
  I want each mounted root to have a canonical, collision-free identity that names its own channel and to let multiple consumers share that identity transparently
  So that two roots of different modules can coexist on one socket and several React components can observe the same root without app-level coordination

  Background:
    Given a client with an open Musubi socket

  Rule: The wire root_id is "<module>:<caller id>", composed identically on both sides

    Scenario: The client composes the root_id to name the root's channel
      Given the client mounts module "MyApp.Stores.Inbox" with id "default"
      When the client opens the root's channel
      Then the client composes the wire root_id "MyApp.Stores.Inbox:default"
      And the channel topic is "musubi:connection:MyApp.Stores.Inbox:default"
      And the join payload carries "module", the caller-supplied "id", and "params"
      # The client MUST know the root_id before it has a topic to join, so the
      # composition is part of the protocol, not a server-assigned opaque token.

    Scenario: The join reply confirms the composed root_id
      When the server accepts the join
      Then the server composes the same "<module>:<id>" value
      And the server replies :ok with that value as the "root_id" payload field
      And every "patch" push on this channel carries the same "root_id"

    Scenario: A join without a caller-supplied id is rejected
      Given the client joins a root channel with a payload that has no "id" field
      Then the server replies :error with reason "missing root id"
      And no root page server is started

  Rule: Connection-level identity is the pair (module, caller-supplied id)

    Scenario: Two roots of different modules share the same caller-supplied id
      Given the client mounts module "MyApp.Stores.Inbox" with id "default"
      When the client mounts module "MyApp.Stores.FlowCatalog" with id "default"
      Then the two roots compose distinct root_ids and join two distinct topics
      And each channel owns exactly one root page server
      And patches for the Inbox root are delivered only on the Inbox channel
      And patches for the FlowCatalog root are delivered only on the FlowCatalog channel

  Rule: Joining a root channel is mounting it; leaving is unmounting

    Scenario: The server starts the root on join and emits the initial patch
      When the client joins a declared root's channel
      Then the server starts exactly one root page server bound to that channel
      And the server emits the root's initial patch on that channel

    Scenario: Leaving one root's channel stops only that root
      Given the client has mounted two roots on the same socket
      When the client leaves the first root's channel
      Then the server stops that root's page server via the channel's terminate/2
      And the second root keeps serving commands and patches on its own channel
      # There are no "mount" / "unmount" wire messages: join IS the mount and
      # leave IS the unmount.

  Rule: A second mount of the same (module, id) aliases client-side, with no server round-trip

    Scenario: A second consumer shares the existing root connection
      Given the client has mounted module "MyApp.Stores.Inbox" with id "default" and holds the resulting StoreProxy P1
      When a second consumer on the same connection calls mountStore with the same module and id
      Then the client finds the composed root_id in its local roots Map
      And the client increments that RootConnection's local refCount
      And the second mountStore call resolves to the same StoreProxy P1
      And no second channel is opened and no message is sent to the server
      # Multi-observer ergonomic: two React components on the same connection
      # observing the same root share one channel and one client proxy.

    Scenario: A consumer that aliases a mount still in flight waits for the initial patch
      Given a first mountStore call is awaiting the root's initial patch
      When a second consumer calls mountStore for the same (module, id)
      Then the second call awaits the same in-flight initial patch
      And a rejected join un-does the second consumer's refCount increment
      And the caller never observes a not-yet-connected store

  Rule: The last release leaves the channel after a brief grace window

    Scenario: The grace timer fires and tears down when no new consumer arrives
      Given the client has one consumer holding the only ref on a mounted root
      When that consumer calls unmount and no remount arrives within the grace window
      Then the grace timer fires
      And the client removes the entry from its local roots Map
      And the client leaves the root's channel
      And the server stops that root's page server via terminate/2

    Scenario: A re-mount within the grace window cancels the pending teardown
      Given the client has one consumer holding the only ref on a mounted root
      When that consumer calls unmount
      Then the client does NOT immediately leave the channel
      And a grace timer is scheduled
      When a new consumer calls mountStore for the same (module, id) before the timer fires
      Then the client aliases to the same RootConnection and bumps refCount back to 1
      And the grace timer is cancelled
      And the channel is never left, so the server root is never stopped
      # Covers React 19 route-swap commit batching and StrictMode effect replay.

  Rule: Dev-mode warns when params mismatch on an alias

    Scenario: Two consumers alias to the same root with different params
      Given the first mountStore call used params {filter: "unread"}
      And the second mountStore call uses params {filter: "all"} with the same (module, id)
      When the client aliases the second call to the existing RootConnection
      Then in NODE_ENV != "production" a console.warn message is emitted
      And the warning explains first-mount params are authoritative and advises a distinct id for separate instances
      And the existing RootConnection's params are NOT mutated
