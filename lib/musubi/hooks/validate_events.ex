defmodule Musubi.Hooks.ValidateEvents do
  @moduledoc """
  Default `:after_serialize` hook that validates a store socket's drained push
  events against that store's declared `event` schema.

  Attached to the `:after_serialize` transform stage per store socket, so it
  receives (and returns unchanged) the wire-form `Musubi.Page.Frame` and
  validates `frame.events` against `socket.module` — each store validates its own
  events. Dev-correctness only, mirroring `Musubi.Hooks.ValidateRender`: present
  in `:dev`/`:test` via `config :musubi, :default_hooks`, absent in `:prod`. A
  declared event whose payload is missing a field or has a type mismatch raises
  `ArgumentError` (let-it-crash); an undeclared event name is skipped.
  Not a security boundary — events are server-pushed.
  """

  alias Musubi.Event
  alias Musubi.Page.Frame
  alias Musubi.Socket

  @doc """
  Validates `frame.events` against `socket.module`'s declared events, returning
  the frame unchanged.

  ## Examples

      Musubi.Hooks.ValidateEvents.after_serialize(%Musubi.Page.Frame{events: []}, socket)
      #=> {:cont, %Musubi.Page.Frame{events: []}, socket}
  """
  @spec after_serialize(Frame.t(), Socket.t()) :: {:cont, Frame.t(), Socket.t()}
  def after_serialize(%Frame{events: events} = frame, %Socket{module: module} = socket)
      when is_atom(module) do
    Event.validate_events!(events, module)
    {:cont, frame, socket}
  end
end
