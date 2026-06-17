defmodule Musubi.AsyncResultTest do
  use ExUnit.Case, async: true

  alias Musubi.AsyncResult

  doctest AsyncResult

  describe "loading/0" do
    test "returns the empty loading struct" do
      assert %AsyncResult{status: :loading, result: nil, reason: nil} = AsyncResult.loading()
    end
  end

  describe "loading/1" do
    test "preserves the prior result from an AsyncResult" do
      prior = %AsyncResult{status: :ok, result: "snapshot", reason: nil}

      assert %AsyncResult{status: :loading, result: "snapshot", reason: nil} =
               AsyncResult.loading(prior)
    end

    test "uses raw values directly as the prior result" do
      assert %AsyncResult{status: :loading, result: "raw", reason: nil} =
               AsyncResult.loading("raw")
    end

    test "nil prior keeps result nil" do
      assert %AsyncResult{status: :loading, result: nil, reason: nil} = AsyncResult.loading(nil)
    end
  end

  describe "ok/2" do
    test "replaces prior with the new value" do
      prior = AsyncResult.loading("snapshot")

      assert %AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil} =
               AsyncResult.ok(prior, %{name: "ada"})
    end
  end

  describe "failed/2" do
    test "preserves prior result on failure" do
      prior = %AsyncResult{status: :ok, result: "snapshot", reason: nil}

      assert %AsyncResult{status: :failed, result: "snapshot", reason: {:error, :timeout}} =
               AsyncResult.failed(prior, {:error, :timeout})
    end

    test "preserves raw prior values" do
      assert %AsyncResult{status: :failed, result: "raw", reason: {:exit, :killed}} =
               AsyncResult.failed("raw", {:exit, :killed})
    end
  end

  describe "wire serialization" do
    test "status atom becomes a string and field keys become string keys" do
      assert %{
               "__musubi_async__" => true,
               "status" => "loading",
               "result" => nil,
               "reason" => nil
             } =
               Musubi.Wire.to_wire(AsyncResult.loading())

      assert %{"__musubi_async__" => true, "status" => "ok", "result" => 42, "reason" => nil} =
               Musubi.Wire.to_wire(AsyncResult.ok(42))
    end
  end
end
