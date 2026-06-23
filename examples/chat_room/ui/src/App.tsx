import { useEffect, useState } from "react"
import type { SubmitEvent } from "react"
import { MusubiCommandError, type StoreProxy } from "@musubi/react"

import {
  CHAT_ROOM_ROOT,
  useMusubiCommand,
  useMusubiRoot,
  useMusubiSnapshot
} from "./musubi"

function formatCommandError(error: unknown, label: string): string {
  if (MusubiCommandError.is(error)) {
    if (error.kind === "timeout") return `${label} timed out`
    return error.code ? `${label} failed: ${error.code}` : error.message
  }
  return error instanceof Error ? error.message : `${label} failed.`
}

type RootModule = "ChatRoom.Stores.ChatRoomStore"
type Store<M extends keyof Musubi.Stores & string> = StoreProxy<M, Musubi.Stores>

export default function App() {
  const rootMount = useMusubiRoot(CHAT_ROOM_ROOT)

  if (rootMount.status === "loading") {
    return <main className="chat-shell">Connecting...</main>
  }

  if (rootMount.status === "error") {
    return <main className="chat-shell">{rootMount.error.message}</main>
  }

  const root = rootMount.store
  return <ChatRoom root={root} />
}

function ChatRoom({ root }: { root: Store<RootModule> }) {
  const room = useMusubiSnapshot(root)

  const setName = useMusubiCommand(root, "set_name")
  const sendMessage = useMusubiCommand(root, "send_message")

  const [nameDraft, setNameDraft] = useState("")
  const [body, setBody] = useState("")
  const [feedback, setFeedback] = useState("")
  const busy = setName.isPending ? "name" : sendMessage.isPending ? "send" : null

  useEffect(() => {
    setNameDraft(room?.current_user.name ?? "")
  }, [room?.current_user.name])

  // Snapshot is `undefined` during the reconnect window (index reset mid-
  // recovery). Show the connecting shell instead of crashing on deref.
  if (!room) return <main className="chat-shell">Connecting...</main>

  const currentUser = room.current_user
  const onlineUsers = room.online_users
  const messages = room.messages
  const onlineCount = onlineUsers.status === "ok" ? onlineUsers.data.length : 0
  const messagesList = messages.data ?? []
  const messagesCount = messagesList.length

  async function handleSetName(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault()

    const nextName = nameDraft.trim()

    if (!nextName) {
      setFeedback("Name cannot be empty.")
      return
    }

    try {
      const reply = await setName.dispatch({ name: nextName })
      setFeedback(`Name updated to ${reply.name}.`)
    } catch (error) {
      setFeedback(formatCommandError(error, "Name update"))
    }
  }

  async function handleSend(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault()

    const nextBody = body.trim()

    if (!nextBody) {
      setFeedback("Message body cannot be empty.")
      return
    }

    try {
      const reply = await sendMessage.dispatch({ body: nextBody })
      setFeedback(reply.queued ? "Message queued for async delivery." : "Send request returned.")
      setBody("")
    } catch (error) {
      setFeedback(formatCommandError(error, "Message send"))
    }
  }

  return (
    <main className="chat-shell">
      <aside className="sidebar" aria-label="Chat room details">
        <div className="room-card">
          <div className="room-mark">#</div>
          <div>
            <p className="eyebrow">Room</p>
            <h1>general</h1>
          </div>
        </div>

        <section className="identity-card" aria-label="Your profile">
          <div className="avatar self-avatar">{initials(currentUser.name)}</div>
          <div className="identity-copy">
            <span>Posting as</span>
            <strong>{currentUser.name}</strong>
          </div>
        </section>

        <form className="name-form" onSubmit={handleSetName}>
          <label className="sr-only" htmlFor="display-name">
            Display name
          </label>
          <input
            id="display-name"
            value={nameDraft}
            onChange={(event) => setNameDraft(event.target.value)}
            placeholder="Display name"
          />
          <button type="submit" disabled={busy === "name"}>
            {busy === "name" ? "Saving" : "Rename"}
          </button>
        </form>

        <section className="presence-panel" aria-label="Online users">
          <div className="section-heading">
            <h2>Online</h2>
            <span className={`status-dot status-${onlineUsers.status}`} />
          </div>

          {onlineUsers.status === "ok" ? (
            <ul className="users">
              {onlineUsers.data.map((user) => (
                <li key={user.id}>
                  <span className="avatar">{initials(user.name)}</span>
                  <span className="user-meta">
                    <strong>{user.name}</strong>
                    <small>{user.id}</small>
                  </span>
                </li>
              ))}
            </ul>
          ) : onlineUsers.status === "loading" ? (
            <p className="side-note">Loading presence</p>
          ) : (
            <p className="side-note">Presence unavailable</p>
          )}
        </section>
      </aside>

      <section className="chatbox" aria-label="Chat messages">
        <header className="chat-header">
          <div>
            <p className="eyebrow">Live chat</p>
            <h2>Chat room</h2>
          </div>
          <div className="chat-stats" aria-label="Room activity">
            <span>{onlineCount} online</span>
            <span>{messagesCount} messages</span>
            <span className={`status-dot status-${messages.status}`} aria-label={`history ${messages.status}`} />
          </div>
        </header>

        <div className="messages-viewport">
          {messages.status === "loading" && messagesCount === 0 ? (
            <div className="empty-state">
              <div className="empty-mark">…</div>
              <p>Loading history</p>
            </div>
          ) : messages.status === "failed" && messagesCount === 0 ? (
            <div className="empty-state">
              <div className="empty-mark">!</div>
              <p>Could not load history.</p>
            </div>
          ) : messagesCount === 0 ? (
            <div className="empty-state">
              <div className="empty-mark">+</div>
              <p>No messages yet.</p>
            </div>
          ) : (
            <ol className="messages">
              {messagesList.map((message) => {
                const fromSelf = message.sender === currentUser.name

                return (
                  <li
                    key={message.id}
                    className={fromSelf ? "message message-self" : "message"}
                  >
                    <span className="avatar">{initials(message.sender)}</span>
                    <article className="bubble">
                      <header>
                        <strong>{message.sender}</strong>
                        <small>{shortMessageId(message.id)}</small>
                      </header>
                      <p>{message.body}</p>
                    </article>
                  </li>
                )
              })}
            </ol>
          )}
        </div>

        <footer className="composer-dock">
          <div className="send-state" aria-live="polite">
            {feedback || renderSendStatus(room.last_send_status)}
          </div>
          <form className="message-form" onSubmit={handleSend}>
            <label className="sr-only" htmlFor="message-body">
              Message
            </label>
            <input
              id="message-body"
              value={body}
              onChange={(event) => setBody(event.target.value)}
              placeholder="Write a message"
            />
            <button type="submit" disabled={busy === "send"}>
              {busy === "send" ? "Sending" : "Send"}
            </button>
          </form>
        </footer>
      </section>
    </main>
  )
}

function renderSendStatus(status: {
  type: "idle" | "ok" | "failed"
  id?: string
  reason?: string
}): string {
  switch (status.type) {
    case "idle":
      return "idle"
    case "ok":
      return `ok (${status.id ?? ""})`
    case "failed":
      return `failed (${status.reason ?? ""})`
  }
}

function initials(name: string): string {
  const letters = name
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("")

  return letters || "?"
}

function shortMessageId(id: string): string {
  return id.length > 10 ? id.slice(-10) : id
}
