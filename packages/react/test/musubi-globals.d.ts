// Stand-in for the generated codegen bundle. Mirrors
// `packages/client/test/musubi-globals.d.ts`. Tests load this ambient
// declaration so `Musubi.StoreDef` resolves without running the codegen
// pass.
declare namespace Musubi {
  const Type: unique symbol

  interface StoreDef<Module extends string, Shape, Commands, Events = {}> {
    readonly [Type]: {
      module: Module
      shape: Shape
      commands: Commands
      events: Events
    }
  }

  type StoreField<Module extends string> = {
    readonly [Type]: { kind: "store"; module: Module }
  }

  type StreamField<Item> = {
    readonly [Type]: { kind: "stream"; item: Item }
  }

  type AsyncField<Value> = {
    readonly [Type]: { kind: "async"; value: Value }
  }
}
