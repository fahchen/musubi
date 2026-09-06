defmodule Musubi.WireCapture.Scenarios do
  @moduledoc false
  # The scenario list `mix musubi.capture_wire` writes out, one JSON file per
  # entry. Covers `docs/rust-client.md` §12's "at minimum" list plus the upload
  # control plane that rides the connection channel.
  #
  # Each scenario is a `{name, fun}` pair; `fun` receives a fresh
  # `Musubi.WireCapture.Recorder` and returns it after driving the channel.

  alias Musubi.WireCapture.Recorder
  alias Musubi.WireCapture.Stores

  @doc "Every scenario, sorted by name so the written set is stable."
  @spec all() :: [{String.t(), (Recorder.t() -> Recorder.t())}]
  def all do
    Enum.sort_by(
      mount_scenarios() ++
        command_scenarios() ++
        stream_scenarios() ++
        async_and_event_scenarios() ++
        upload_scenarios(),
      &elem(&1, 0)
    )
  end

  defp mount_scenarios do
    [
      # Join IS the mount: one whole-root `replace ""` at base_version 0,
      # version 1, with the child store node inlined.
      {"initial_mount",
       fn recorder ->
         Recorder.join(recorder, Stores.AlphaRootStore, "alpha-1", %{"room_id" => "general"})
       end},

      # A second mount of the same module after a leave: the root restarts at
      # version 0, so the client sees another whole-root replace. This is the
      # recovery shape BDR-0015 relies on.
      {"root_replace_on_rejoin",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.AlphaRootStore, "alpha-1", %{"room_id" => "general"})
         |> Recorder.leave()
         |> Recorder.join(Stores.AlphaRootStore, "alpha-1", %{"room_id" => "second"})
       end},

      # The join reply carries the rejection and no channel is opened.
      {"mount_rejected_unknown_root",
       fn recorder ->
         Recorder.join(recorder, Musubi.WireCapture.Stores.ChildStore, "nope", %{})
       end},

      # BDR-0011 prune: the child appears on one cycle and is gone the next.
      {"child_mount_unmount",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.ToggleRootStore, "toggle-1", %{})
         |> Recorder.command([], "toggle", %{"show" => true})
         |> Recorder.command([], "toggle", %{"show" => false})
       end},

      # A real dropped push: every frame is server-authored, the middle patch
      # is simply not delivered, so the client sees base_version 2 while it
      # holds version 1 and must keep its last good document.
      {"version_gap",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.MetaRootStore, "meta-1", %{})
         |> Recorder.snapshot()
         |> Recorder.command([], "put", %{"key" => "b", "value" => "2"})
         |> Recorder.command([], "put", %{"key" => "c", "value" => "3"})
         # `in` frames so far: join reply, join patch, put-b reply, put-b patch,
         # put-c reply, put-c patch. Dropping index 3 leaves the client at
         # version 1 when the version-2-based envelope arrives.
         |> Recorder.drop_in_frame(3)
       end}
    ]
  end

  defp command_scenarios do
    [
      # `{:noreply, socket}` with an assign change: empty ok reply, then the
      # incremental `replace` ops for both the root and the child.
      {"command_noreply_replace",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.AlphaRootStore, "alpha-1", %{"room_id" => "general"})
         |> Recorder.command([], "rename", %{"room_id" => "random"})
       end},

      # `{:reply, map, socket}` with no assign change: the reply carries data
      # and the idle cycle emits nothing (BDR-0018).
      {"command_reply_no_patch",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.BetaRootStore, "beta-1", %{"label" => "secondary"})
         |> Recorder.command([], "echo", %{"label" => "hello"})
       end},

      # `add` then `remove` over a `map()` field's keys.
      {"command_add_remove_ops",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.MetaRootStore, "meta-1", %{})
         |> Recorder.command([], "put", %{"key" => "b", "value" => "2"})
         |> Recorder.command([], "drop", %{"key" => "a"})
       end},

      # Both command error replies, on a root that survives each of them.
      {"command_errors",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.AlphaRootStore, "alpha-1", %{"room_id" => "general"})
         |> Recorder.command(["child"], "missing", %{})
         |> Recorder.push("command", %{"store_id" => [], "payload" => %{"room_id" => "x"}})
         |> Recorder.command([], "rename", %{"room_id" => "still-mounted"})
       end}
    ]
  end

  defp stream_scenarios do
    [
      # `reset` ahead of the inserts.
      {"stream_reset",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.StreamRootStore, "stream-1", %{})
         |> Recorder.command([], "seed", %{"count" => 3})
       end},

      # Append (`at: -1`) and prepend (`at: 0`) against a seeded stream.
      {"stream_insert",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.StreamRootStore, "stream-1", %{})
         |> Recorder.command([], "seed", %{"count" => 2})
         |> Recorder.command([], "insert", %{"id" => "9", "at" => -1, "limit" => nil})
         |> Recorder.command([], "insert", %{"id" => "0", "at" => 0, "limit" => nil})
       end},
      {"stream_delete",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.StreamRootStore, "stream-1", %{})
         |> Recorder.command([], "seed", %{"count" => 3})
         |> Recorder.command([], "delete", %{"id" => "2"})
       end},

      # Explicit indices plus the upsert case: re-inserting an existing
      # item_key repositions it rather than duplicating it (client-side).
      {"stream_at_variants",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.StreamRootStore, "stream-1", %{})
         |> Recorder.command([], "seed", %{"count" => 3})
         |> Recorder.command([], "insert", %{"id" => "7", "at" => 1, "limit" => nil})
         |> Recorder.command([], "insert", %{"id" => "1", "at" => 2, "limit" => nil})
       end},

      # `limit: 0` (drop everything), a positive limit (trim from the front)
      # and a negative one (trim from the tail after a head insert).
      {"stream_limit_variants",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.StreamRootStore, "stream-1", %{})
         |> Recorder.command([], "seed", %{"count" => 4})
         |> Recorder.command([], "insert", %{"id" => "5", "at" => -1, "limit" => 3})
         |> Recorder.command([], "insert", %{"id" => "6", "at" => 0, "limit" => -2})
         |> Recorder.command([], "insert", %{"id" => "7", "at" => -1, "limit" => 0})
       end}
    ]
  end

  defp async_and_event_scenarios do
    [
      {"async_loading_ok",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.AsyncRootStore, "async-1", %{})
         |> Recorder.command([], "load", %{"outcome" => "ok"})
         |> Recorder.await()
       end},
      {"async_loading_failed",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.AsyncRootStore, "async-1", %{})
         |> Recorder.command([], "load", %{"outcome" => "failed"})
         |> Recorder.await()
       end},

      # BDR-0032 + BDR-0018: an envelope carrying `events` and no `ops`.
      {"event_only_cycle",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.EventRootStore, "event-1", %{})
         |> Recorder.command([], "notify", %{"msg" => "saved"})
       end}
    ]
  end

  defp upload_scenarios do
    [
      # Preflight accepted: the reply carries the config plus one external-mode
      # entry, and the next envelope carries the `config` and `add` upload ops.
      {"upload_preflight_ok",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.UploadRootStore, "upload-1", %{})
         |> Recorder.push("allow_upload", %{
           "store_id" => [],
           "name" => "avatar",
           "entries" => [entry("0", "a.png", 2048)]
         })
       end},

      # Two rejections in one preflight: a disallowed extension and an
      # oversized file. Neither reaches the state tree.
      {"upload_preflight_rejected",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.UploadRootStore, "upload-1", %{})
         |> Recorder.push("allow_upload", %{
           "store_id" => [],
           "name" => "avatar",
           "entries" => [
             entry("0", "a.gif", 2048),
             entry("1", "big.png", 5_000_000)
           ]
         })
       end},

      # External-uploader mode: `upload_progress` drives the `progress` ops and
      # the final 100 flips the entry to done.
      {"upload_progress_complete",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.UploadRootStore, "upload-1", %{})
         |> Recorder.push("allow_upload", %{
           "store_id" => [],
           "name" => "avatar",
           "entries" => [entry("0", "a.png", 2048)]
         })
         |> progress("avatar", 50)
         # `progress` ops are throttled to 10 Hz server-side
         # (`upload_progress_last_emitted`), so the second push has to land
         # more than 100ms after the first or its op is coalesced away.
         |> Recorder.await()
         |> progress("avatar", 100)
       end},
      {"upload_cancel",
       fn recorder ->
         recorder
         |> Recorder.join(Stores.UploadRootStore, "upload-1", %{})
         |> Recorder.push("allow_upload", %{
           "store_id" => [],
           "name" => "avatar",
           "entries" => [entry("0", "a.png", 2048)]
         })
         |> cancel("avatar")
       end}
    ]
  end

  defp entry(client_ref, name, size) do
    %{"client_ref" => client_ref, "name" => name, "size" => size, "type" => "image/png"}
  end

  # The entry ref is server-issued, so it has to be read back out of the
  # preflight reply the recorder just captured.
  defp progress(recorder, name, percent) do
    Recorder.push(recorder, "upload_progress", %{
      "store_id" => [],
      "name" => name,
      "ref" => last_entry_ref(recorder),
      "progress" => percent
    })
  end

  defp cancel(recorder, name) do
    Recorder.push(recorder, "cancel_upload", %{
      "store_id" => [],
      "name" => name,
      "ref" => last_entry_ref(recorder)
    })
  end

  defp last_entry_ref(%Recorder{frames: frames}) do
    Enum.find_value(frames, fn
      %{"dir" => "in", "event" => "phx_reply", "payload" => %{"response" => response}} ->
        response |> Map.get("entries", %{}) |> Map.values() |> List.first() |> entry_ref()

      _frame ->
        nil
    end)
  end

  defp entry_ref(%{"entry_ref" => ref}), do: ref
  defp entry_ref(_other), do: nil
end
