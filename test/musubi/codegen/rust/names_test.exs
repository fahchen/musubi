defmodule Musubi.Codegen.Rust.NamesTest do
  use ExUnit.Case, async: true

  alias Musubi.Codegen.Rust.Names

  doctest Musubi.Codegen.Rust.Names

  describe "field_ident/1" do
    test "plain snake_case atom passes through with no rename" do
      assert {"title", nil} = Names.field_ident(:title)
    end

    test "raw-identifier keywords get `r#` and no rename (serde strips the prefix)" do
      keywords = [
        :type,
        :move,
        :ref,
        :match,
        :fn,
        :use,
        :mod,
        :impl,
        :where,
        :mut,
        :abstract,
        :async,
        :await,
        :dyn,
        :try,
        :gen
      ]

      for keyword <- keywords do
        expected = "r#" <> Atom.to_string(keyword)

        assert {^expected, nil} = Names.field_ident(keyword)
      end
    end

    test "keywords that cannot be raw get a trailing underscore plus a rename" do
      assert {"self_", "self"} = Names.field_ident(:self)
      assert {"Self_", "Self"} = Names.field_ident(:Self)
      assert {"super_", "super"} = Names.field_ident(:super)
      assert {"crate_", "crate"} = Names.field_ident(:crate)
      assert {"__", "_"} = Names.field_ident(:_)
    end

    test "binary map keys that are already idents need no rename" do
      assert {"key", nil} = Names.field_ident("key")
    end

    test "non-ident binary map keys are sanitized and renamed" do
      assert {"kebab_key", "kebab-key"} = Names.field_ident("kebab-key")
      assert {"with_space", "with space"} = Names.field_ident("with space")
      assert {"_2fa", "2fa"} = Names.field_ident("2fa")
    end
  end

  describe "variant_ident/1" do
    test "snake_case atom becomes PascalCase with an explicit rename" do
      assert {"CheckingOut", "checking_out"} = Names.variant_ident(:checking_out)
    end

    test "single-word atom" do
      assert {"Idle", "idle"} = Names.variant_ident(:idle)
    end

    test "atoms carrying arbitrary characters keep the wire form in the rename" do
      assert {"NeedsReview", "needs-review"} = Names.variant_ident(:"needs-review")
      assert {"WithSpace", "with space"} = Names.variant_ident(:"with space")
    end

    test "leading digit is prefixed so the variant is a valid ident" do
      assert {"_2fa", "2fa"} = Names.variant_ident(:"2fa")
    end

    test "a variant colliding with a non-raw keyword gets a trailing underscore" do
      assert {"Self_", "Self"} = Names.variant_ident(:Self)
    end
  end

  describe "pascal_case/1" do
    test "pascal_case splits on every non-alphanumeric separator" do
      assert Names.pascal_case(:line_items) == "LineItems"
      assert Names.pascal_case("needs-review") == "NeedsReview"
      assert Names.pascal_case("a.b c") == "ABC"
    end

    test "pascal_case preserves inner casing" do
      assert Names.pascal_case("cartID") == "CartID"
    end
  end

  describe "module and struct paths" do
    test "module_path/1 snake_cases every segment (kind: :store modules)" do
      assert Names.module_path(MyApp.Stores.CartStore) == "my_app::stores::cart_store"
    end

    test "module_path/1 accepts expanded alias segments" do
      assert Names.module_path([:MyApp, :Stores, :CartStore]) == "my_app::stores::cart_store"
    end

    test "struct_path/1 keeps the last segment verbatim (kind: :state modules)" do
      assert Names.struct_path(MyApp.States.CartState) == "my_app::states::CartState"
      assert Names.struct_path([:Demo, :LineItem]) == "demo::LineItem"
    end

    test "struct_path/1 of a single-segment module is the bare struct name" do
      assert Names.struct_path([:Cart]) == "Cart"
    end

    test "mod_ident/1 escapes a segment that underscores to a raw-able keyword" do
      assert Names.mod_ident(:Match) == "r#match"
      assert Names.mod_ident(:Type) == "r#type"
      assert Names.module_path([:MyApp, :Match, :CartStore]) == "my_app::r#match::cart_store"
    end

    test "mod_ident/1 raises on a keyword that cannot be a raw identifier" do
      for segment <- [:Self, :Super, :Crate] do
        assert_raise ArgumentError, ~r/reserved Rust module name/, fn ->
          Names.mod_ident(segment)
        end
      end
    end

    test "struct_ident/1 raises on `Self`, which cannot name a type" do
      assert Names.struct_ident(:CartState) == "CartState"

      assert_raise ArgumentError, ~r/`Self` is reserved/, fn -> Names.struct_ident(:Self) end
    end
  end

  describe "hoisted_name/2" do
    test "concatenates the enclosing item name with the PascalCased path" do
      assert Names.hoisted_name("CartState", [:address]) == "CartStateAddress"
      assert Names.hoisted_name("CartState", [:meta, :tags]) == "CartStateMetaTags"
    end

    test "binary path segments are PascalCased the same way" do
      assert Names.hoisted_name("Probe", [:status, "needs-review"]) == "ProbeStatusNeedsReview"
    end

    test "an empty path is the enclosing item name itself" do
      assert Names.hoisted_name("CartState", []) == "CartState"
    end
  end

  describe "allocate/2" do
    test "a free name is claimed verbatim" do
      assert {"CartStateAddress", claimed} = Names.allocate("CartStateAddress", MapSet.new())
      assert MapSet.member?(claimed, "CartStateAddress")
    end

    test "a taken name gets numeric suffixes, never an error" do
      claimed = MapSet.new(["Dup"])

      assert {"Dup2", claimed} = Names.allocate("Dup", claimed)
      assert {"Dup3", claimed} = Names.allocate("Dup", claimed)
      assert {"Dup4", _claimed} = Names.allocate("Dup", claimed)
    end

    test "suffixed names that are themselves taken are skipped" do
      assert {"Dup3", _claimed} = Names.allocate("Dup", MapSet.new(["Dup", "Dup2"]))
    end
  end
end
