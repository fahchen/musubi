# `:rust` — the codegen compile smoke test (docs/rust-codegen.md §6.5) shells
# out to `cargo check`. Excluded by default so plain `mix test` and
# `mix precommit` never need a Rust toolchain; run it with `mix test --only rust`.
ExUnit.start(capture_log: true, exclude: [:rust])
