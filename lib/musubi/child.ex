defmodule Musubi.Child do
  @moduledoc "Render-time child placeholder sentinel resolved by `Musubi.Resolver` when it appears in store output."

  use TypedStructor

  @type assigns_map() :: map()

  typed_structor do
    field :module, module(),
      enforce: true,
      doc: "Child store module to mount or update at this position."

    field :id, String.t() | nil,
      default: nil,
      doc:
        "Child node id (must be a binary). Combined with `parent_path` and `module` forms the runtime identity tuple."

    field :assigns, assigns_map(),
      default: %{},
      doc:
        "Parent-supplied assigns flowed into the child via `child(Module, id: ..., key: value, ...)`. The keys here form the child's consumed-key set used for memoization."
  end

  @doc """
  Builds a child placeholder sentinel.

  The runtime treats the returned struct specially only when it appears inside a
  `render/1` return value. Elsewhere it is inert data.

  ## Examples

      iex> sentinel = Musubi.Child.child(MyStore, id: "sidebar", title: "Inbox")
      iex> sentinel.module
      MyStore
      iex> sentinel.id
      "sidebar"
      iex> sentinel.assigns
      %{title: "Inbox"}
  """
  @spec child(module(), keyword()) :: t()
  def child(module, opts \\ []) when is_atom(module) and is_list(opts) do
    {id, assigns_opts} = Keyword.pop(opts, :id)

    %__MODULE__{
      module: module,
      id: id,
      assigns: Map.new(assigns_opts)
    }
  end
end
