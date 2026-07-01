defmodule Musubi.Hooks.ValidateEvents do
  @moduledoc """
  Default `:before_events` hook that validates a cycle's drained push-event
  payloads against the root store's declared `event` schemas (BDR-0032).

  Dev-correctness only, mirroring `Musubi.Hooks.ValidateRender`: attached in
  `:dev`/`:test` via `config :musubi, :default_hooks`, absent in `:prod`. A
  declared event whose payload is missing a field or has a type mismatch raises
  `ArgumentError` (BDR-0003 let-it-crash); an undeclared event name is skipped.
  This is **not** a security boundary — events are server-pushed. The event
  list is returned unchanged; this hook only observes.
  """

  alias Musubi.Event
  alias Musubi.Socket

  @doc """
  Validates each event's payload against `socket.module`'s declared events.

  `socket` is the root socket, so `socket.module` is the root store where events
  are declared. Returns `{:cont, events, socket}` with `events` untouched;
  raises on a declared-event payload mismatch.

  ## Examples

      Musubi.Hooks.ValidateEvents.before_events([%{name: "toast", payload: %{}}], socket)
      #=> {:cont, [%{name: "toast", payload: %{}}], socket}
  """
  @spec before_events([Event.event()], Socket.t()) :: {:cont, [Event.event()], Socket.t()}
  def before_events(events, %Socket{module: root_module} = socket)
      when is_list(events) and is_atom(root_module) do
    Event.validate_events!(events, root_module)
    {:cont, events, socket}
  end
end
