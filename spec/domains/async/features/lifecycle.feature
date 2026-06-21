@async @lifecycle
Feature: Async Task Lifecycle
  As a store author
  I want to launch background tasks whose results integrate with socket.assigns and the wire shape via Musubi.AsyncResult
  So that long-running work runs off the runtime mailbox and clients can pattern-match on a status enum (:loading | :ok | :failed)

  Background:
    Given a connected client
    And a page runtime mounted on the client's transport session

  Rule: Musubi.AsyncResult is a three-field struct keyed on a status enum

    Scenario: Loading default
      When code calls Musubi.AsyncResult.loading()
      Then the struct is %{status: :loading, result: nil, reason: nil}

    Scenario: Loading preserves the prior result for stale-while-loading UX
      Given a prior result of %AsyncResult{status: :ok, result: "snapshot"}
      When code calls Musubi.AsyncResult.loading(prior)
      Then the struct is %{status: :loading, result: "snapshot", reason: nil}

    Scenario: Ok with a value
      When code calls Musubi.AsyncResult.ok(prior, value)
      Then the struct is %{status: :ok, result: value, reason: nil}

    Scenario: Failed preserves the prior result for stale-while-failed UX
      Given a prior result of "snapshot"
      When code calls Musubi.AsyncResult.failed(prior, {:error, reason})
      Then the struct is %{status: :failed, result: "snapshot", reason: {:error, reason}}

  Rule: assign_async writes loading synchronously and resolves to ok or failed when the task completes

    Scenario: Single key happy path
      When the application calls assign_async(socket, :profile, fn -> {:ok, %{profile: data}} end)
      Then socket.assigns.profile is set to Musubi.AsyncResult.loading() immediately
      And on task completion socket.assigns.profile becomes Musubi.AsyncResult.ok(prior, data)

    Scenario: Multi-key atomic write
      When the application calls assign_async(socket, [:user, :org], fn -> {:ok, %{user: u, org: o}} end)
      Then both keys are set to Musubi.AsyncResult.loading() immediately
      And on task completion both keys are updated atomically

    Scenario: User function returns invalid shape
      When the user function returns {:invalid, :shape}
      Then the runtime raises ArgumentError inside the task and writes Musubi.AsyncResult.failed(prior, {:exit, ...})

    Scenario: User function returns explicit error
      When the user function returns {:error, :unauthorized}
      Then socket.assigns.<key> becomes Musubi.AsyncResult.failed(prior, {:error, :unauthorized})

  Rule: start_async routes results to handle_async; AsyncResult is not auto-written

    Scenario: Result delivered to handle_async
      Given a store implements handle_async(:warm_cache, result, socket)
      When the application calls start_async(socket, :warm_cache, fn -> Cache.warm() end)
      Then the runtime spawns a task and tracks it under name :warm_cache
      And on completion handle_async(:warm_cache, {:ok, val}, socket) is invoked

    Scenario: No automatic AsyncResult assignment
      When the application calls start_async(socket, :warm_cache, fn -> ... end)
      Then socket.assigns is unchanged by the call
      And the client sees no AsyncResult unless the application manually writes one in handle_async

  Rule: handle_async must return {:noreply, socket}

    Scenario: Successful return
      When handle_async(:foo, {:ok, val}, socket) returns {:noreply, updated_ctx}
      Then the runtime accepts the new socket and triggers a render cycle

    Scenario: Other return shapes raise
      When handle_async/3 returns anything other than {:noreply, socket}
      Then the runtime raises with a "bad callback response" error

  Rule: cancel_async actively terminates a task and writes failed when called via AsyncResult variant

    Scenario: Cancel by AsyncResult sets failed before killing the task
      When the application calls cancel_async(socket, %AsyncResult{status: :loading} = ar, :user_navigated_away)
      Then socket.assigns.profile is updated to Musubi.AsyncResult.failed(ar, {:exit, :user_navigated_away})
      And the runtime kills the associated task pid

    Scenario: Cancel by key kills the task and lets DOWN drive the failed write
      When the application calls cancel_async(socket, :profile, :user_navigated_away)
      Then the runtime kills the associated task pid
      And the resulting :DOWN message triggers socket.assigns.profile = Musubi.AsyncResult.failed(prior, {:exit, :user_navigated_away})

  Rule: assign_async :reset re-emits loading without killing the prior task (LiveView parity)

    Scenario: Reset all keys
      Given the prior assign_async for [:user, :org] is still in flight
      When the application calls assign_async(socket, [:user, :org], fun, reset: true)
      Then the prior task is left running and its result lazy-discards by ref
      And both keys re-emit Musubi.AsyncResult.loading()

    Scenario: Reset subset of keys
      Given the prior assign_async for [:user, :org] is still in flight
      When the application calls assign_async(socket, [:user, :org], fun, reset: [:user])
      Then the prior task is left running and its result lazy-discards by ref
      And :user re-emits Musubi.AsyncResult.loading(); :org preserves its prior loading state unchanged

    Scenario: No reset preserves prior result during reload
      Given socket.assigns.profile is %AsyncResult{status: :ok, result: prior_data}
      When the application calls assign_async(socket, :profile, fun) without :reset
      Then socket.assigns.profile becomes %AsyncResult{status: :loading, result: prior_data, reason: nil}
      And the prior result stays visible to the client until the new task completes

  Rule: Tasks are linked to the page runtime; runtime exit kills tasks; default supervisor is Musubi.AsyncSupervisor

    Scenario: Runtime exits
      Given async tasks are running
      When the page runtime exits
      Then all tasks linked to it exit with it
      And no orphan task survives

    Scenario: Custom supervisor
      When the application passes :supervisor to assign_async or start_async
      Then the task starts under that supervisor instead of Musubi.AsyncSupervisor

  Rule: A second start_async with the same name silently overwrites the prior tracked ref

    Scenario: Old task lazy-discards
      Given start_async(socket, :foo, task_a) is in flight
      When the application calls start_async(socket, :foo, task_b) before task_a completes
      Then runtime tracking switches to task_b's ref
      And task_a continues running but its result is discarded on arrival

  Rule: Cancel-vs-completion races resolve first-to-arrive-wins via ref-prune

    Scenario: Result wins
      Given a task is cancellable and nearly complete
      When the result message reaches the runtime before the cancel signal
      Then the runtime writes ok and prunes the ref
      And the subsequent cancel finds no entry and is a no-op

    Scenario: Cancel wins
      Given the runtime processes the cancel before the task's result arrives
      Then the runtime kills the pid and writes failed
      And the late result message is dropped because the ref no longer matches

  Rule: A :timeout option terminates an overdue task and produces failed: {:exit, :timeout}

    Scenario: Timer fires before completion
      When the application calls assign_async(socket, :profile, fun, timeout: 5_000)
      And the task does not complete within 5 seconds
      Then the runtime kills the task pid
      And socket.assigns.profile becomes Musubi.AsyncResult.failed(prior, {:exit, :timeout})

    Scenario: Task completes before timer fires
      When a task with timeout: 5_000 completes in 2 seconds
      Then the timer is cancelled
      And socket.assigns.<key> reflects the normal ok or failed result

  Rule: Result classification on the wire AsyncResult is deterministic

    Scenario Outline: Status enum mapping
      Given a task encounters <event>
      Then socket.assigns.<key> is updated to <terminal_state>

      Examples:
        | event                                      | terminal_state                                           |
        | user fun returns {:ok, val}                | status: :ok, result: val, reason: nil                    |
        | user fun returns {:error, reason}          | status: :failed, result: prior_or_nil, reason: {:error, reason} |
        | task raises an exception                   | status: :failed, reason: {:exit, {reason, stacktrace}}   |
        | task throws                                | status: :failed, reason: {:exit, {{:nocatch, ...}, st}}  |
        | task process exits with reason r           | status: :failed, reason: {:exit, r}                      |
        | timeout fires                              | status: :failed, reason: {:exit, :timeout}               |
        | cancel_async with reason r                 | status: :failed, reason: {:exit, r}                      |

  Rule: A store node disappearing does not actively cancel its async tasks; results are lazily discarded

    Scenario: Child unmounted mid-task
      Given a child store has issued assign_async
      When the parent's next render no longer includes the child
      Then the child's task continues running
      And on completion the runtime checks the registry
      And finding the originating node absent, the runtime emits [:musubi, :async, :lazy_discard] and writes nothing to assigns

  Rule: AsyncResult.of(T) is a compile-time typespec marker

    Scenario: Field declaration
      When a store declares field :profile, AsyncResult.of(UserProfileState.t())
      Then codegen emits a discriminated-union TypeScript shape keyed on status (:loading | :ok | :failed)
      And the runtime validator accepts %Musubi.AsyncResult{} or structurally-equivalent maps

  Rule: AsyncResult serializes the status atom as a string on the wire

    Scenario: Wire shape uses string-coerced status
      Given a Musubi.AsyncResult struct with fields status, result, reason
      When the runtime serializes it for the JSON Patch payload
      Then the resulting JSON object uses keys "status", "result", "reason"
      And the status atom (:loading | :ok | :failed) becomes the string ("loading" | "ok" | "failed")

  Rule: User functions warned for socket capture (LV-aligned)

    Scenario: Closure captures socket
      When a developer writes assign_async(socket, :foo, fn -> socket.assigns.bar end)
      Then the compile-time validator emits a warning recommending an explicit local binding before the fn

  Rule: AsyncResult flows through JSON Patch like ordinary maps

    Scenario: AsyncResult transitions on the wire
      Given socket.assigns.profile transitions loading -> ok -> loading -> ok
      Then patch envelopes contain replace ops at /profile/loading, /profile/ok, /profile/result
      And the wire shape is the JSON-serialized AsyncResult struct

  Rule: handle_async/3 exceptions are caught; runtime survives

    Scenario: handle_async raises
      Given handle_async(:foo, {:ok, val}, socket) raises a KeyError
      Then the runtime catches the exception
      And emits [:musubi, :async, :exception] with kind, reason, stacktrace
      And the runtime continues to process subsequent messages
      And socket.assigns is not modified for that cycle

  Rule: Async telemetry is a Musubi extension over LV

    Scenario Outline: Lifecycle event surface
      When the runtime processes an async event
      Then it emits a telemetry event of name "<event>"
      With metadata including page_id, store_id, name (or keys), kind (assign or start)

      Examples:
        | event                        |
        | [:musubi, :async, :start]     |
        | [:musubi, :async, :stop]      |
        | [:musubi, :async, :exception] |
        | [:musubi, :async, :cancel]    |
        | [:musubi, :async, :lazy_discard] |

  Rule: Mount-time assign_async produces loading state in the first envelope

    Scenario: Mount calls assign_async
      Given mount(socket) calls assign_async(socket, :profile, fun)
      When the first patch envelope is emitted
      Then the value at /profile in the initial replace is %{status: "loading", result: null, reason: null}
      And a subsequent envelope's ops update /profile/status and /profile/result on completion
