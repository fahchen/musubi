defmodule Musubi.Plugin.Codegen do
  @moduledoc """
  TypedStructor plugin that marks a Musubi `state do` block as eligible for
  client codegen.

  The plugin injects an `@after_compile` callback pointing at
  `Musubi.Codegen.Manifest`, which serializes the field, command and event
  reflection into a per-module manifest entry under
  `Mix.Project.build_path()/musubi-codegen/`. The codegen Mix compilers —
  `:musubi_ts` and `:musubi_rust` — then discover eligible modules by listing
  those entries; there is no beam scan or `:application.get_key/2` walk.

  The manifest payload is target-neutral, so a single `@after_compile` stamp
  feeds every renderer: `Musubi.Codegen.TypeScript` and `Musubi.Codegen.Rust`
  both read it. This plugin is wired into the typed_structor block built by
  `Musubi.DSL.State.state/1`.
  """

  use TypedStructor.Plugin

  @impl TypedStructor.Plugin
  @spec init(keyword()) :: :ok
  defmacro init(_opts), do: :ok

  @impl TypedStructor.Plugin
  defmacro after_definition(_definition, _opts) do
    quote do
      @after_compile {Musubi.Codegen.Manifest, :__after_compile__}
    end
  end
end
