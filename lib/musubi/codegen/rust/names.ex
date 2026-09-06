defmodule Musubi.Codegen.Rust.Names do
  @moduledoc """
  Pure naming helpers for the Rust codegen target: identifier escaping, case
  conversion, generated module/type paths, and hoisted-name allocation.

  Split out of `Musubi.Codegen.Rust.TypeRenderer` because these are the rules
  with non-obvious edge cases — keyword escaping and collision suffixes — and
  they deserve a table test of their own. The TypeScript target needs no
  counterpart: TS is structural, so it never names an anonymous shape.

  ## Identifiers

  Field names come from Elixir atoms and are already snake_case, so the
  generated ident is normally the atom verbatim with no `#[serde(rename)]`.
  Three exceptions:

  | Input                       | Ident       | `#[serde(rename = ...)]` |
  | :-------------------------- | :---------- | :----------------------- |
  | `:name`                     | `name`      | none                     |
  | Rust keyword (`:type`)      | `r#type`    | none (serde strips `r#`) |
  | Non-raw keyword (`:self`)   | `self_`     | `"self"`                 |
  | Non-ident key (`"a-b"`)     | `a_b`       | `"a-b"`                  |

  ## Module segments

  `mod_ident/1` applies the same escaping to `pub mod` names: an Elixir
  namespace segment that snake_cases to a Rust keyword is emitted as `r#<kw>`,
  and the four keywords that cannot be raw identifiers raise. `struct_ident/1`
  does the same for the leaf struct name, where only `Self` is reserved.
  """

  # Keywords that accept the `r#` raw-identifier prefix. Serde's derive strips
  # `r#` when deriving the wire name, so `r#type` serializes as `"type"` with
  # no rename attribute. Includes the edition-reserved keywords.
  @raw_keywords ~w(
    abstract as box break const continue do else enum extern false final fn for
    if impl in let loop macro match mod move mut override priv pub ref return
    static struct trait true try type typeof unsafe unsized use virtual where
    while yield async await become dyn gen
  )

  # Keywords that cannot be raw identifiers (plus the `_` wildcard). They get a
  # trailing underscore and an explicit serde rename back to the wire name.
  @non_raw_keywords ~w(self Self super crate _)

  @doc """
  Returns the Rust field identifier for `name` and the `#[serde(rename)]` value
  it needs, or `nil` when the identifier already matches the wire key.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.field_ident(:title)
      {"title", nil}
      iex> Musubi.Codegen.Rust.Names.field_ident(:type)
      {"r#type", nil}
      iex> Musubi.Codegen.Rust.Names.field_ident(:self)
      {"self_", "self"}
      iex> Musubi.Codegen.Rust.Names.field_ident("kebab-key")
      {"kebab_key", "kebab-key"}
  """
  @spec field_ident(atom() | String.t()) :: {String.t(), String.t() | nil}
  def field_ident(name) when is_atom(name), do: field_ident(Atom.to_string(name))

  def field_ident(name) when is_binary(name) do
    cond do
      name in @non_raw_keywords -> {name <> "_", name}
      name in @raw_keywords -> {"r#" <> name, nil}
      ident?(name) -> {name, nil}
      true -> {sanitize(name), name}
    end
  end

  defp ident?(name) do
    String.match?(name, ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/)
  end

  defp sanitize(name) do
    name
    |> String.replace(~r/[^A-Za-z0-9]+/, "_")
    |> prefix_leading_digit()
    |> case do
      "" -> "_"
      sanitized -> sanitized
    end
  end

  defp prefix_leading_digit(<<first, _rest::binary>> = name) when first in ?0..?9, do: "_" <> name
  defp prefix_leading_digit(name), do: name

  @doc """
  Returns the Rust enum variant identifier for the atom literal `name` and the
  wire string it renames to. The rename is always emitted — atoms carry
  arbitrary characters, so a container `rename_all` would silently mis-map them.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.variant_ident(:checking_out)
      {"CheckingOut", "checking_out"}
      iex> Musubi.Codegen.Rust.Names.variant_ident(:"needs-review")
      {"NeedsReview", "needs-review"}
  """
  @spec variant_ident(atom() | String.t()) :: {String.t(), String.t()}
  def variant_ident(name) when is_atom(name), do: variant_ident(Atom.to_string(name))

  def variant_ident(name) when is_binary(name) do
    variant =
      case pascal_case(name) do
        keyword when keyword in @non_raw_keywords -> keyword <> "_"
        "" -> "_"
        pascal -> pascal
      end

    {variant, name}
  end

  @doc """
  Converts `name` to PascalCase, dropping every non-alphanumeric separator.
  Unlike `Macro.camelize/1` this also splits on characters an Elixir atom may
  carry (`-`, spaces), which reach the generator through atom literals and
  binary map keys.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.pascal_case(:line_items)
      "LineItems"
      iex> Musubi.Codegen.Rust.Names.pascal_case("needs-review")
      "NeedsReview"
      iex> Musubi.Codegen.Rust.Names.pascal_case("2fa")
      "_2fa"
  """
  @spec pascal_case(atom() | String.t()) :: String.t()
  def pascal_case(name) do
    name
    |> to_string()
    |> String.split(~r/[^A-Za-z0-9]+/, trim: true)
    |> Enum.map_join("", &capitalize_first/1)
    |> prefix_leading_digit()
  end

  defp capitalize_first(<<first::utf8, rest::binary>>), do: String.upcase(<<first::utf8>>) <> rest
  defp capitalize_first(""), do: ""

  @doc """
  Returns the Rust `pub mod` segment for one Elixir namespace segment:
  `Macro.underscore/1` plus raw-identifier escaping.

  Raises `ArgumentError` for the four keywords that cannot carry the `r#`
  prefix (`self`, `Self`, `super`, `crate`) and for the `_` wildcard: a module
  path is consumer-visible, so silently rewriting it is worse than asking for
  the Elixir module to be renamed.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.mod_ident(:CartStore)
      "cart_store"
      iex> Musubi.Codegen.Rust.Names.mod_ident(:Match)
      "r#match"
  """
  @spec mod_ident(atom() | String.t()) :: String.t()
  def mod_ident(segment) do
    case segment |> to_string() |> Macro.underscore() do
      keyword when keyword in @non_raw_keywords ->
        raise ArgumentError,
              "Musubi Rust codegen: module segment #{inspect(to_string(segment))} becomes the " <>
                "reserved Rust module name #{inspect(keyword)}, which cannot be a raw " <>
                "identifier; rename the Elixir module"

      keyword when keyword in @raw_keywords ->
        "r#" <> keyword

      ident ->
        ident
    end
  end

  @doc """
  Returns the Rust struct name for the last segment of a `kind: :state` module,
  kept verbatim. Raises `ArgumentError` on `Self`, the one Rust type name that
  cannot be declared and cannot be escaped.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.struct_ident(:CartState)
      "CartState"
  """
  @spec struct_ident(atom() | String.t()) :: String.t()
  def struct_ident(name) do
    case to_string(name) do
      "Self" ->
        raise ArgumentError,
              "Musubi Rust codegen: `Self` is reserved and cannot name a generated struct; " <>
                "rename the Elixir module"

      ident ->
        ident
    end
  end

  @doc """
  Returns the `::`-joined Rust module path for an Elixir module — every segment
  snake_cased. Used for `kind: :store` modules, which always get their own
  `pub mod`.

  Accepts a module atom or the already-split segments an expanded
  `{:__aliases__, _, parts}` node carries.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.module_path(MyApp.Stores.CartStore)
      "my_app::stores::cart_store"
      iex> Musubi.Codegen.Rust.Names.module_path([:MyApp, :Stores, :CartStore])
      "my_app::stores::cart_store"
      iex> Musubi.Codegen.Rust.Names.module_path([:MyApp, :Match])
      "my_app::r#match"
  """
  @spec module_path(module() | [atom() | String.t()]) :: String.t()
  def module_path(module) when is_atom(module), do: module |> Module.split() |> module_path()

  def module_path(segments) when is_list(segments),
    do: Enum.map_join(segments, "::", &mod_ident/1)

  @doc """
  Returns the Rust path of the struct generated for a `kind: :state` module:
  every segment but the last snake_cased as a module, the last kept verbatim as
  the struct name.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.struct_path(MyApp.States.CartState)
      "my_app::states::CartState"
      iex> Musubi.Codegen.Rust.Names.struct_path([:Demo, :LineItem])
      "demo::LineItem"
  """
  @spec struct_path(module() | [atom() | String.t()]) :: String.t()
  def struct_path(module) when is_atom(module), do: module |> Module.split() |> struct_path()

  def struct_path(segments) when is_list(segments) do
    {parents, [last]} = Enum.split(segments, -1)

    Enum.map_join(parents, "", &(mod_ident(&1) <> "::")) <> struct_ident(last)
  end

  @doc """
  Builds the deterministic hoisted-type name: the enclosing generated item's
  name followed by the PascalCased path segments walked to reach the anonymous
  shape, outermost first. Wrappers (`list`, `stream`, `AsyncResult.of`, the
  `Option` from `nil`-stripping) contribute no segment, so they never reach
  this function.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.hoisted_name("CartState", [:address])
      "CartStateAddress"
      iex> Musubi.Codegen.Rust.Names.hoisted_name("CartState", [:meta, :tags])
      "CartStateMetaTags"
  """
  @spec hoisted_name(String.t(), [atom() | String.t()]) :: String.t()
  def hoisted_name(prefix, path) do
    prefix <> Enum.map_join(path, "", &pascal_case/1)
  end

  @doc """
  Claims `name` in `claimed`, appending `2`, then `3`, … while the name is
  already taken. Returns the allocated name and the updated claim set.

  Top-level generated item names are claimed first by the bundle assembler, so
  a hoisted type can never shadow one.

  ## Examples

      iex> Musubi.Codegen.Rust.Names.allocate("CartStateMetaTags", MapSet.new())
      {"CartStateMetaTags", MapSet.new(["CartStateMetaTags"])}
      iex> {name, _claimed} = Musubi.Codegen.Rust.Names.allocate("Dup", MapSet.new(["Dup"]))
      iex> name
      "Dup2"
  """
  @spec allocate(String.t(), MapSet.t(String.t())) :: {String.t(), MapSet.t(String.t())}
  def allocate(name, claimed) do
    allocated = next_free(name, claimed, 1)

    {allocated, MapSet.put(claimed, allocated)}
  end

  defp next_free(name, claimed, 1) do
    if MapSet.member?(claimed, name), do: next_free(name, claimed, 2), else: name
  end

  defp next_free(name, claimed, attempt) do
    candidate = name <> Integer.to_string(attempt)

    if MapSet.member?(claimed, candidate),
      do: next_free(name, claimed, attempt + 1),
      else: candidate
  end
end
