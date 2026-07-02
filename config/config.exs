import Config

# Default lifecycle hooks attached to every page server's root socket.
#
# Each entry is `{id, stage, hook_fun}` matching `Musubi.Lifecycle.attach_hook/4`.
#
# Override in an application's own config to disable validation
# (`[]`), replace the validator, or stack additional hooks.

# `:before_command` schema validation runs in every environment so malformed
# payloads crash the runtime per BDR-0003 (let-it-crash) instead of reaching
# user-defined `handle_command/3` clauses with the wrong shape.
command_schema_hook =
  {Musubi.Hooks.ValidateCommandSchema, :before_command,
   &Musubi.Hooks.ValidateCommandSchema.before_command/3}

# `:after_command` reply schema validation. Walks the declared
# `reply_fields` against the actual reply map; reply shape passing
# through `{:halt, reply, _}` from `:before_command` skips this hook
# because the runtime short-circuits past `:after_command` on halt.
reply_schema_hook =
  {Musubi.Hooks.ValidateReplySchema, :after_command,
   &Musubi.Hooks.ValidateReplySchema.after_command/4}

# `Musubi.Stream` drain+prune is NOT a hook — it's a runtime invariant baked
# into `Musubi.Resolver.resolve/2` after the `:after_serialize` hooks run.
# Hooks are user-removable; pending stream ops MUST flush every cycle, so the
# prune step lives in the runtime.

# Dev/test-only render validation hook (`:after_serialize`, over `frame.render`).
# Root-only: the root's frame carries the whole stitched wire tree, and its
# validator recurses into child slots — validating a child's frame standalone
# would reject legitimately-transient async/loading render. Raises in dev/test,
# absent in prod — detach/replace via an app's own `:default_hooks`.
render_validation_hooks =
  if config_env() in [:dev, :test] do
    [
      {Musubi.Hooks.ValidateRender, :after_serialize,
       &Musubi.Hooks.ValidateRender.after_serialize(:raise, &1, &2)}
    ]
  else
    []
  end

config :musubi,
       :default_hooks,
       [command_schema_hook, reply_schema_hook | render_validation_hooks]

# Per-store hooks attached to every store socket (root + children), so each store
# validates its own concerns. `ValidateEvents` (`:after_serialize`, over
# `frame.events`, BDR-0032) checks a store's push-event payloads against its
# declared `event` schema. Dev/test only, detach/replace via `:store_hooks`.
config :musubi,
       :store_hooks,
       if(config_env() in [:dev, :test],
         do: [
           {Musubi.Hooks.ValidateEvents, :after_serialize,
            &Musubi.Hooks.ValidateEvents.after_serialize/2}
         ],
         else: []
       )

if File.exists?(Path.join(__DIR__, "#{config_env()}.exs")) do
  import_config "#{config_env()}.exs"
end
