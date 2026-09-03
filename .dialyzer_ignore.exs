# Add false-positive Dialyzer warnings here.
#
# Each entry can be a regex, a `{file, warning_type}` tuple, or
# `{file, warning_type, line}` — see https://hexdocs.pm/dialyxir for the
# full grammar.
#
# Example (OTP 28 MapSet opaque-type false positive):
#   ~r/call_with(out)?_opaque.*opaque term/,
[
  # `Phoenix.ChannelTest.join/4` destructures `%Phoenix.Socket{transport:
  # {Phoenix.ChannelTest, sup}}`, a tuple, while `Phoenix.Socket.t()` declares
  # `transport: atom`. Dialyzer therefore gives `subscribe_and_join/4` the
  # success typing `none()`, and every caller inherits `no_return`. The rest of
  # the suite is `.exs` and never analysed; the wire-capture harness lives in
  # `test/support` (a Mix task has to reach it) and so is. Nothing here is
  # actionable from this side.
  ~r{^test/support/wire_capture/(recorder|scenarios)\.ex:}
]
