import { useMemo, useState } from "react"
import type { SubmitEvent } from "react"
import { MusubiCommandError, keyOf, type StoreProxy } from "@musubi/react"

import {
  CART_PAGE_ROOT,
  useMusubiCommand,
  useMusubiRoot,
  useMusubiSnapshot
} from "./musubi"

function formatCommandError(error: unknown, label: string): string {
  if (MusubiCommandError.is(error)) {
    if (error.kind === "timeout") return `${label} timed out`
    if (error.code) return `${label} failed: ${error.code}`
    const wrapped = (error.reply as { result?: { error?: string } } | undefined)?.result?.error
    if (wrapped) return `${label} failed: ${wrapped}`
    return error.message
  }
  return error instanceof Error ? error.message : `${label} failed.`
}

type RootModule = "CartPage.Stores.CartPageStore"
type Store<M extends keyof Musubi.Stores & string> = StoreProxy<M, Musubi.Stores>

const PRODUCT_OPTIONS = [
  {
    sku: "mug",
    label: "Coffee Mug",
    detail: "Ceramic desk cup",
    priceCents: 1_500,
    tone: "clay"
  },
  {
    sku: "notebook",
    label: "Notebook",
    detail: "Dot-grid field book",
    priceCents: 800,
    tone: "ink"
  },
  {
    sku: "stickers",
    label: "Sticker Pack",
    detail: "Die-cut labels",
    priceCents: 500,
    tone: "mint"
  }
] as const

export default function App() {
  const rootMount = useMusubiRoot(CART_PAGE_ROOT)

  if (rootMount.status === "loading") {
    return <main className="shell">Connecting...</main>
  }

  if (rootMount.status === "error") {
    return <main className="shell">{rootMount.error.message}</main>
  }

  return <CartPage root={rootMount.store} />
}

function CartPage({ root }: { root: Store<RootModule> }) {
  const page = useMusubiSnapshot(root)

  const cartProxy = root.cart
  const addItem = useMusubiCommand(cartProxy, "add_item")
  const removeLine = useMusubiCommand(cartProxy, "remove_line")
  const checkout = useMusubiCommand(cartProxy, "checkout")

  const [sku, setSku] = useState<(typeof PRODUCT_OPTIONS)[number]["sku"]>("mug")
  const [feedback, setFeedback] = useState<string>("")
  const busy = addItem.isPending
    ? "add"
    : checkout.isPending
      ? "checkout"
      : removeLine.isPending
        ? "remove"
        : null

  const selectedProduct = useMemo(
    () => PRODUCT_OPTIONS.find((option) => option.sku === sku) ?? PRODUCT_OPTIONS[0],
    [sku]
  )

  const headerLabel = useMemo(() => {
    const header = page?.header
    if (!header) {
      return "Connecting to Musubi..."
    }

    if (!header.signed_in) {
      return "Guest checkout is disabled"
    }

    return `Signed in as ${header.user_name ?? "Unknown"}`
  }, [page?.header])

  // Snapshot is `undefined` during the reconnect window (index reset mid-
  // recovery). Show the connecting shell instead of crashing on deref.
  if (!page) return <main className="shell">Connecting...</main>

  async function handleAddItem(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault()
    try {
      const reply = await addItem.dispatch({ sku })
      setFeedback(
        "ok" in reply.result
          ? `Added ${selectedProduct.label} to demo-cart.`
          : `Add failed: ${reply.result.error}`
      )
    } catch (error) {
      setFeedback(formatCommandError(error, "Add"))
    }
  }

  async function handleCheckout() {
    try {
      const reply = await checkout.dispatch({})

      if ("order_id" in reply.result) {
        setFeedback(`Checkout succeeded: ${reply.result.order_id}`)
      } else {
        setFeedback(`Checkout blocked: ${reply.result.error}`)
      }
    } catch (error) {
      setFeedback(formatCommandError(error, "Checkout"))
    }
  }

  async function handleRemoveLine(id: string) {
    try {
      await removeLine.dispatch({ id })
      setFeedback("Line removed.")
    } catch (error) {
      setFeedback(formatCommandError(error, "Remove"))
    }
  }

  return (
    <main className="shell">
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Musubi Storefront Runtime</p>
          <h1>Cart control room</h1>
          <p>
            A Musubi store driving a React cart with server-owned state, command replies,
            persistence, and reconnect recovery.
          </p>
        </div>

        <div className="hero-metrics" aria-label="Cart quantity summary">
          <div>
            <span className="metric-label">Connection</span>
            <strong>{page.header?.signed_in ? "Signed in" : "Guest"}</strong>
          </div>
          <div>
            <span className="metric-label">Product types</span>
            <strong>{page.cart.lines.length}</strong>
          </div>
          <div>
            <span className="metric-label">Total units</span>
            <strong>{page.cart.total_units}</strong>
          </div>
        </div>
      </header>

      <section className="connection-strip" aria-label="Runtime notes">
        <p>{headerLabel}</p>
        <p>
          Cart id <code>demo-cart</code>
        </p>
        <p>Open a second tab to watch the ETS-backed cart synchronize live.</p>
      </section>

      <div className="workspace">
        <section className="catalog" aria-labelledby="catalog-heading">
          <div className="section-heading">
            <p className="eyebrow">Command target: cart</p>
            <h2 id="catalog-heading">Add product</h2>
          </div>

          <form className="catalog-form" onSubmit={handleAddItem}>
            <fieldset>
              <legend className="sr-only">Choose a product SKU</legend>
              <div className="product-grid">
                {PRODUCT_OPTIONS.map((option) => (
                  <label
                    key={option.sku}
                    className="product-card"
                    data-tone={option.tone}
                    data-selected={option.sku === sku}
                    onClick={() => setSku(option.sku)}
                  >
                    <input
                      type="radio"
                      name="sku"
                      value={option.sku}
                      checked={option.sku === sku}
                      onChange={() => setSku(option.sku)}
                    />
                    <span className="product-art" aria-hidden="true">
                      <span />
                    </span>
                    <span className="product-copy">
                      <strong>{option.label}</strong>
                      <span>{option.detail}</span>
                    </span>
                    <span className="product-price">{formatMoney(option.priceCents)}</span>
                  </label>
                ))}
              </div>
            </fieldset>

            <div className="command-bar">
              <div>
                <span className="metric-label">Selected SKU</span>
                <strong>{selectedProduct.sku}</strong>
              </div>
              <button type="submit" disabled={busy === "add"}>
                {busy === "add" ? "Adding..." : `Add ${selectedProduct.label}`}
              </button>
            </div>
          </form>
        </section>

        <section className="cart-panel" aria-labelledby="cart-heading">
          <div className="cart-header">
            <div>
              <p className="eyebrow">Server snapshot</p>
              <h2 id="cart-heading">Cart lines</h2>
            </div>
            <span className="status-pill" data-status={page.cart.status.type}>
              {formatStatus(page.cart.status.type)}
            </span>
          </div>

          {page.cart.lines.length === 0 ? (
            <div className="empty">
              <strong>No lines yet</strong>
              <p>Add a product to watch the store tree update through Musubi.</p>
            </div>
          ) : (
            <ul className="lines">
              {cartProxy.lines.map((lineProxy) => (
                <CartLine
                  key={keyOf(lineProxy)}
                  lineProxy={lineProxy}
                  busy={busy}
                  onRemove={(id) => void handleRemoveLine(id)}
                  onFeedback={setFeedback}
                />
              ))}
            </ul>
          )}

          <div className="checkout">
            <div>
              <span className="metric-label">Subtotal</span>
              <strong>{formatMoney(page.cart.subtotal_cents)}</strong>
            </div>
            <button
              type="button"
              onClick={() => void handleCheckout()}
              disabled={busy === "checkout" || page.cart.lines.length === 0}
            >
              {busy === "checkout" ? "Checking out..." : "Checkout"}
            </button>
          </div>

          {page.cart.status.type === "checked_out" ? (
            <p className="notice">Last order id: {page.cart.status.order_id}</p>
          ) : null}

          {feedback ? (
            <p className="notice" role="status" aria-live="polite">
              {feedback}
            </p>
          ) : null}
        </section>
      </div>
    </main>
  )
}

type LineProxy = Store<"CartPage.Stores.CartLineStore">

function CartLine({
  lineProxy,
  busy,
  onRemove,
  onFeedback
}: {
  lineProxy: LineProxy
  busy: "add" | "checkout" | "remove" | null
  onRemove: (id: string) => void
  onFeedback: (message: string) => void
}) {
  const line = useMusubiSnapshot(lineProxy)
  const incQty = useMusubiCommand(lineProxy, "inc_qty")
  const decQty = useMusubiCommand(lineProxy, "dec_qty")
  const pending = incQty.isPending ? "inc" : decQty.isPending ? "dec" : null

  // Child snapshot is `undefined` during the reconnect window; drop the row
  // rather than crash. The parent shell renders its own skeleton.
  if (!line) return null

  const step = async (kind: "inc" | "dec") => {
    try {
      const reply = await (kind === "inc" ? incQty.dispatch({}) : decQty.dispatch({}))
      onFeedback(`Line ${line.sku} -> qty ${reply.qty}`)
    } catch (error) {
      onFeedback(formatCommandError(error, "Qty update"))
    }
  }

  return (
    <li className="line">
      <div className="line-main">
        <strong>{line.name}</strong>
        <span>
          {line.sku} / qty {line.qty}
        </span>
      </div>
      <div className="line-actions">
        <div className="qty-stepper" role="group" aria-label={`Quantity for ${line.name}`}>
          <button
            type="button"
            className="ghost"
            onClick={() => void step("dec")}
            disabled={pending !== null || line.qty <= 1}
            aria-label="Decrease quantity"
          >
            −
          </button>
          <span className="qty-readout" aria-live="polite">
            {line.qty}
          </span>
          <button
            type="button"
            className="ghost"
            onClick={() => void step("inc")}
            disabled={pending !== null}
            aria-label="Increase quantity"
          >
            +
          </button>
        </div>
        <span>{formatMoney(line.price_cents * line.qty)}</span>
        <button
          type="button"
          className="ghost"
          onClick={() => onRemove(line.id)}
          disabled={busy === "remove"}
        >
          Remove
        </button>
      </div>
    </li>
  )
}

function formatStatus(status: string): string {
  return status
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function formatMoney(cents: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD"
  }).format(cents / 100)
}
