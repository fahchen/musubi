defmodule Musubi.Hooks.ValidateReplySchemaTest do
  use ExUnit.Case, async: true

  alias Musubi.Hooks.ValidateCommandSchema
  alias Musubi.Hooks.ValidateReplySchema
  alias Musubi.Socket

  defmodule TargetStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :no_reply do
      payload do
        field :ok, boolean()
      end
    end

    command :with_reply do
      reply do
        field :ok, boolean()
        field :name, String.t()
      end
    end

    command :status_reply do
      reply do
        field :status, :active
      end
    end

    command :nested_reply do
      reply do
        field :meta, %{count: integer()}
      end
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{ok: true}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule HostStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{ok: true}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defp host_socket_targeting(target_module) do
    Socket.put_private(
      %Socket{module: HostStore, assigns: %{}, private: %{}},
      ValidateCommandSchema.target_private_key(),
      target_module
    )
  end

  describe "no declared reply fields" do
    test "passes when handler returns an empty reply (noreply path)" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(:no_reply, %{"ok" => true}, %{}, socket)
    end

    test "passes silently when handler returns a non-empty reply" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :no_reply,
                 %{"ok" => true},
                 %{"extra" => "ignored"},
                 socket
               )
    end
  end

  describe "declared reply fields" do
    test "validates a conforming reply" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :with_reply,
                 %{},
                 %{"ok" => true, "name" => "Alice"},
                 socket
               )
    end

    test "accepts atom-keyed reply" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :with_reply,
                 %{},
                 %{ok: true, name: "Alice"},
                 socket
               )
    end

    test "validates atom-valued reply against its wire-form string" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :status_reply,
                 %{},
                 %{status: :active},
                 socket
               )
    end

    test "validates a nested atom-keyed reply against its wire-form shape" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :nested_reply,
                 %{},
                 %{meta: %{count: 3}},
                 socket
               )
    end

    test "raises on missing reply field when handler returns {:noreply, _}" do
      socket = host_socket_targeting(TargetStore)

      assert_raise ArgumentError, ~r/with_reply.*missing required field/s, fn ->
        ValidateReplySchema.after_command(:with_reply, %{}, %{}, socket)
      end
    end

    test "raises on type mismatch" do
      socket = host_socket_targeting(TargetStore)

      assert_raise ArgumentError, ~r/with_reply.*ok: expected boolean/s, fn ->
        ValidateReplySchema.after_command(
          :with_reply,
          %{},
          %{"ok" => "yes", "name" => "Alice"},
          socket
        )
      end
    end
  end

  describe "fall-through" do
    test "unknown command on the addressed module is a no-op" do
      socket = host_socket_targeting(TargetStore)

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(:not_declared, %{}, %{}, socket)
    end

    test "no addressed module configured falls back to socket module" do
      socket = %Socket{module: TargetStore, assigns: %{}, private: %{}}

      assert {:cont, ^socket} =
               ValidateReplySchema.after_command(
                 :with_reply,
                 %{},
                 %{"ok" => true, "name" => "Alice"},
                 socket
               )
    end
  end

  test "successful validation emits [:musubi, :validate, :reply, :stop]" do
    ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :validate, :reply, :stop]])
    on_exit(fn -> :telemetry.detach(ref) end)

    socket = host_socket_targeting(TargetStore)

    assert {:cont, ^socket} =
             ValidateReplySchema.after_command(
               :with_reply,
               %{},
               %{"ok" => true, "name" => "Alice"},
               socket
             )

    assert_receive {[:musubi, :validate, :reply, :stop], ^ref, %{count: 1},
                    %{store_module: TargetStore, command: :with_reply}}
  end
end
