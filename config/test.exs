import Config

# Test-only TS codegen output path. The integration test for
# `Mix.Tasks.Compile.MusubiTs` (`test/mix/tasks/compile/musubi_ts_test.exs`)
# drives the compiler end-to-end against this path. Tests own cleanup.
config :musubi, :ts_codegen_output_path, "test/tmp/musubi_ts_bundle.ts"

# Test-only Rust codegen output path. The integration test for
# `Mix.Tasks.Compile.MusubiRust` (`test/mix/tasks/compile/musubi_rust_test.exs`)
# drives the compiler end-to-end against this path. Tests own cleanup.
config :musubi, :rust_codegen_output_path, "test/tmp/musubi_rust_bundle.rs"

# Test-only endpoint config for the Phoenix Channel adapter test
# (`test/musubi/transport/channel_test.exs`). The endpoint is defined inside the
# test module so the keys here track that module's full name. `server: false`
# keeps the endpoint from binding any port.
config :musubi, Musubi.Transport.ChannelTest.TestEndpoint,
  pubsub_server: Musubi.Transport.ChannelTest.PubSub,
  secret_key_base: String.duplicate("a", 64),
  server: false

# Endpoint for the shared wire-capture harness in `test/support/wire_capture/`,
# driven both by `test/musubi/transport/connection_channel_test.exs` and by
# `mix musubi.capture_wire`. `server: false` keeps it from binding a port.
config :musubi, Musubi.WireCapture.Endpoint,
  pubsub_server: Musubi.WireCapture.PubSub,
  secret_key_base: String.duplicate("a", 64),
  server: false
