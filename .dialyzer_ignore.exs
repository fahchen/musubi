# Add false-positive Dialyzer warnings here.
#
# Each entry can be a regex, a `{file, warning_type}` tuple, or
# `{file, warning_type, line}` — see https://hexdocs.pm/dialyxir for the
# full grammar.
#
# Example (OTP 28 MapSet opaque-type false positive):
#   ~r/call_with(out)?_opaque.*opaque term/,
[
  # All five entries below are one upstream defect. `Phoenix.Socket.t()`
  # declares `channel_pid: pid`, `topic: String.t()` and `transport: atom`, but
  # the struct a `connect/3` callback is actually handed carries `nil`, `nil`
  # and `{Phoenix.ChannelTest, sup}` in those fields — Phoenix's own
  # `Phoenix.Socket.__connect__/6` and `Phoenix.ChannelTest.join/4` build it
  # that way, so no value can satisfy the contract before a join. The
  # wire-capture harness is the only `test/support` code Dialyzer analyses (a
  # Mix task has to reach it, so it is `.ex`; the rest of the suite is `.exs`
  # and is never compiled into the app), which is why the mismatch surfaces
  # only here. Pinned per warning type rather than per file: any *other* kind
  # of warning in these two files still fails the build.

  # Root cause, and the only non-derived warning of the five:
  # `Musubi.WireCapture.Socket.connect/3` is called with the hand-built
  # pre-join socket described above, which cannot match `Phoenix.Socket.t()`.
  {"test/support/wire_capture/recorder.ex", :call},
  # Fallout: that call types `connect/0` as `none()`, and its sole caller
  # `join/4` inherits it.
  {"test/support/wire_capture/recorder.ex", :no_return},
  # Fallout: `join/4`'s truthful `@spec ... :: t()` cannot match a `none()`
  # success typing.
  {"test/support/wire_capture/recorder.ex", :invalid_contract},
  # Fallout one module out: every scenario closure bottoms out in
  # `Recorder.join/4`.
  {"test/support/wire_capture/scenarios.ex", :no_return},
  # Fallout: the upload helpers are reached only from those closures, so
  # Dialyzer reads them as unreachable.
  {"test/support/wire_capture/scenarios.ex", :unused_fun}
]
