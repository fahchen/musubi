defmodule ChatRoom.MixProject do
  use Mix.Project

  def project do
    [
      app: :chat_room,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: false,
      compilers: Mix.compilers() ++ [:musubi_ts, :musubi_rust],
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ChatRoom.Application, []}
    ]
  end

  defp deps do
    [
      {:musubi, path: "../.."},
      {:phoenix, "~> 1.8"},
      {:phoenix_pubsub, "~> 2.1"},
      {:bandit, "~> 1.0"},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      server: ["deps.get", "run --no-halt"],
      ui: [&ui_setup/1, &ui_dev/1],
      desktop: [&desktop_run/1]
    ]
  end

  defp ui_setup(_args), do: cmd!("pnpm install", "ui")
  defp ui_dev(_args), do: cmd!("pnpm dev", "ui")

  # `cargo run` resolves and builds on its own, so there is no `cargo fetch`
  # step. A cold cache compiles gpui from source: expect a minute or two the
  # first time, seconds afterwards.
  defp desktop_run(_args) do
    Mix.shell().info("Building the gpui client; the first run compiles gpui (~1-2 minutes).")
    cmd!("cargo run", "desktop")
  end

  defp cmd!(command, dir) do
    case Mix.shell().cmd(command, cd: dir) do
      0 -> :ok
      status -> Mix.raise("`#{command}` exited with status #{status}")
    end
  end
end
