defmodule Musubi.SocketTest do
  use ExUnit.Case, async: true

  alias Musubi.Socket

  describe "struct defaults" do
    test "matches the frozen socket shape" do
      socket = %Socket{}

      assert socket.assigns == %{}
      assert socket.id == nil
      assert socket.parent_path == []
      assert socket.module == nil
      assert socket.endpoint == nil
      assert socket.topic == nil
      assert socket.transport_pid == nil
      assert socket.private == %{}
    end
  end

  describe "assign helpers" do
    test "assign/3 records changes only when the value differs by ===" do
      socket = Socket.assign(%Socket{}, :status, :ready)

      assert socket.assigns.status == :ready
      assert socket.assigns.__changed__ == %{status: true}
      assert Socket.changed?(socket, :status)

      unchanged = Socket.assign(socket, :status, :ready)

      assert unchanged == socket
      assert unchanged.assigns.__changed__ == %{status: true}
    end

    test "assign/3 creates a nil-valued key on an absent key" do
      socket = Socket.assign(%Socket{}, :cursor, nil)

      assert Map.has_key?(socket.assigns, :cursor)
      assert socket.assigns[:cursor] == nil
      assert socket.assigns.__changed__ == %{cursor: true}
    end

    test "assign/3 short-circuits re-assigning nil to an existing nil key" do
      socket = Socket.reset_changed(Socket.assign(%Socket{}, :cursor, nil))
      unchanged = Socket.assign(socket, :cursor, nil)

      assert unchanged == socket
      assert unchanged.assigns.__changed__ == %{}
    end

    test "assign/2 handles maps and keyword lists" do
      socket =
        %Socket{}
        |> Socket.assign(%{title: "Inbox"})
        |> Socket.assign(unread_count: 3)

      assert socket.assigns.title == "Inbox"
      assert socket.assigns.unread_count == 3
      assert socket.assigns.__changed__ == %{title: true, unread_count: true}
    end

    test "update/3 is chainable" do
      socket =
        %Socket{}
        |> Socket.assign(:count, 1)
        |> Socket.update(:count, &(&1 + 1))
        |> Socket.update(:count, &(&1 + 1))

      assert socket.assigns.count == 3
      assert socket.assigns.__changed__ == %{count: true}
    end

    test "assign_new/3 sets the value only when the key is absent" do
      socket = Socket.assign_new(%Socket{}, :count, fn -> 0 end)
      assert socket.assigns.count == 0
      assert socket.assigns.__changed__ == %{count: true}

      socket = Socket.reset_changed(socket)
      socket = Socket.assign_new(socket, :count, fn -> 99 end)

      assert socket.assigns.count == 0
      refute Socket.changed?(socket, :count)
    end

    test "assign_new/3 does not overwrite a falsy value already present" do
      socket = Socket.assign(%Socket{}, :flag, false)
      assert socket.assigns.flag == false

      socket = Socket.reset_changed(socket)
      socket = Socket.assign_new(socket, :flag, fn -> true end)

      assert socket.assigns.flag == false
      refute Socket.changed?(socket, :flag)
    end

    test "reset_changed/1 clears tracked changes" do
      socket =
        %Socket{}
        |> Socket.assign(:title, "old")
        |> Socket.reset_changed()

      assert socket.assigns.__changed__ == %{}
      refute Socket.changed?(socket, :title)
    end

    test "consumed_keys_changed?/2 only matches consumed keys" do
      socket =
        %Socket{}
        |> Socket.assign(:sibling_field, 1)
        |> Socket.assign(:title, "Inbox")
        |> Socket.reset_changed()
        |> Socket.assign(:sibling_field, 2)

      refute Socket.consumed_keys_changed?(socket, [:title, :unread_count])
      assert Socket.consumed_keys_changed?(socket, [:title, :sibling_field])
    end
  end
end
