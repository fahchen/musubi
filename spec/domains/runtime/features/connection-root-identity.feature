@runtime @transport @connection-root
Feature: Connection Root Identity
  As a connected client mounting one or more root stores on a single Musubi connection
  I want the server to assign each mounted root a canonical, collision-free identity
  So that two roots of different modules can coexist on one connection without aliasing

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

    Scenario: The same (module, id) pair on one connection is rejected
      Given the client mounts module "MyApp.Stores.Inbox" with id "default" and the server replies :ok
      When the client mounts module "MyApp.Stores.Inbox" with id "default" a second time on the same connection
      Then the server replies :error with reason "root already mounted"
      And the previously mounted page runtime keeps running unaffected

  Rule: The client carries no dedup state for connection roots

    Scenario: Two concurrent mountStore calls for the same (module, id) both reach the server
      Given the client has no mounted root for module "MyApp.Stores.Inbox" with id "default"
      When two callers invoke mountStore concurrently with that module and id
      Then the client transports two distinct "mount" pushes
      And the server replies :ok to the first and :error with "root already mounted" to the second
      And only the successful caller receives a usable root proxy
      # Higher-level consumers that want shared mounts across components layer
      # their own ref-counting (e.g. @musubi/react's pendingRootMounts), not
      # the connection client.
