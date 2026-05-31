@runtime @transport @connection-root
Feature: Connection Root Identity
  As a connected client mounting one or more root stores on a single Musubi connection
  I want the server to assign each mounted root a canonical, collision-free identity and to let multiple consumers share that identity transparently
  So that two roots of different modules can coexist on one connection and several React components can observe the same root without app-level coordination

  Background:
    Given a connected client has joined the Musubi connection channel

  Rule: The server is the sole authority on a mounted root's wire id

    Scenario: A successful mount returns a server-assigned root_id
      Given the client sends "mount" with module "MyApp.Stores.Inbox" and caller-supplied id "default"
      When the server starts the page runtime
      Then the server replies :ok with a "root_id" payload field
      And the server stores the page runtime under that "root_id" in mounted_roots
      And the client uses the returned "root_id" verbatim on every subsequent payload

    Scenario: The client treats the wire root_id as opaque
      Given the server has returned "root_id" "MyApp.Stores.Inbox:default" for a successful mount
      When the client builds the next "command" / "unmount" / upload / patch payload
      Then the client copies the server-assigned "root_id" without parsing or rebuilding it
      And the client never composes a wire root_id from {module, id} on its own

  Rule: Connection-level identity is the pair (module, caller-supplied id)

    Scenario: Two roots of different modules share the same caller-supplied id
      Given the client mounts module "MyApp.Stores.Inbox" with id "default" and receives root_id R1
      When the client mounts module "MyApp.Stores.FlowCatalog" with id "default"
      Then the server replies :ok with a distinct root_id R2
      And R1 and R2 address two separate page runtimes
      And subsequent patches for R1 are delivered only to the Inbox root proxy
      And subsequent patches for R2 are delivered only to the FlowCatalog root proxy

  Rule: A second mount of the same (module, id) aliases to the existing root

    Scenario: The server replies :already_mounted with the existing root_id and the client aliases
      Given the client has mounted module "MyApp.Stores.Inbox" with id "default" and holds the resulting StoreProxy P1
      When a second consumer on the same connection calls mountStore with the same module and id
      Then the server replies :error with reason "already_mounted" and the existing "root_id" payload field
      And the client looks the returned "root_id" up in its local roots Map and finds the existing RootConnection
      And the client increments that RootConnection's local refCount
      And the second mountStore call resolves to the same StoreProxy P1
      And the server does NOT start a second page runtime
      # Multi-observer ergonomic: two React components on the same connection
      # observing the same root share one server mount and one client proxy.

    Scenario: An :already_mounted reply with a root_id the client does not know is a hard error
      Given the server replies to a mount with reason "already_mounted" and root_id "Some.Root:phantom"
      And the client's local roots Map has no entry for "Some.Root:phantom"
      Then the client throws MusubiInconsistencyError
      And the call does not silently fabricate a RootConnection
      # State drift: the server claims a mount that the client never made.
      # Indicates a real bug (reconnect race, dropped unmount, server-side
      # leak); should surface to the application, not be swallowed.

  Rule: The last unmount tears down the server entry after a brief grace window

    Scenario: A re-mount within the grace window cancels the pending teardown
      Given the client has one consumer holding the only ref on a mounted root
      When that consumer calls unmount
      Then the client does NOT immediately push "unmount" on the wire
      And a grace timer is scheduled
      When a new consumer calls mountStore for the same (module, id) before the timer fires
      And the server replies :already_mounted with the existing root_id
      Then the client aliases to the same RootConnection and bumps refCount back to 1
      And the grace timer is cancelled
      And no "unmount" push is ever emitted

    Scenario: The grace timer fires and tears down when no new consumer arrives
      Given the client has one consumer holding the only ref on a mounted root
      When that consumer calls unmount and no remount arrives within the grace window
      Then the grace timer fires
      And the client pushes "unmount" with the server-assigned "root_id"
      And the server stops the page runtime and removes the entry from mounted_roots
      And the client removes the entry from its local roots Map

  Rule: Dev-mode warns when params mismatch on an alias

    Scenario: Two consumers alias to the same root with different params
      Given the first mountStore call used params {filter: "unread"}
      And the second mountStore call uses params {filter: "all"} with the same (module, id)
      When the client aliases the second call to the existing RootConnection
      Then in NODE_ENV != "production" a console.warn message is emitted
      And the warning explains first-mount params are authoritative and advises a distinct id for separate instances
      And the existing RootConnection's params are NOT mutated
