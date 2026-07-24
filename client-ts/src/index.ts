export const PROTOCOL_VERSION = 1

export type ProtocolVersion = 1 | 2

export type Role = 'client' | 'adapter'
export type CommandType = 'session.send' | 'permission.respond' | 'session.interrupt' | 'session.stop'
export type AckStatus = 'accepted' | 'rejected' | 'duplicate'

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }
export type JsonObject = { [key: string]: JsonValue }

export interface Subscription {
  session_id: string
  last_seq: number
}

export type HelloFrame =
  | {
      frame: 'hello'
      protocol_version: ProtocolVersion
      role: 'client'
      token: string
      subscriptions: Subscription[]
    }
  | {
      frame: 'hello'
      protocol_version: 1
      role: 'adapter'
      token: string
      session_id: string
      provider: string
      resume?: boolean
    }

export interface SessionSummary {
  session_id: string
  state: string
  provider: string
  latest_seq: number
  replay_from: number
}

export interface HelloAckFrame {
  frame: 'hello.ack'
  protocol_version: ProtocolVersion
  sessions: SessionSummary[]
  capabilities?: HelloCapabilities
}

export interface HelloCapabilities {
  history_page?: { max_limit: number }
}

export interface AgentWharfEvent {
  frame: 'event'
  type: string
  session_id: string
  seq?: number
  time: number
  payload: JsonValue
}

export interface CommandFrame {
  frame: 'command'
  cmd_id: string
  type: CommandType
  session_id: string
  payload: JsonValue
}

export interface CommandAckFrame {
  frame: 'command.ack'
  cmd_id: string
  status: AckStatus
  reason: string
}

export interface PingFrame {
  frame: 'ping'
  nonce?: string
}

export interface PongFrame {
  frame: 'pong'
  nonce?: string
}

export interface ErrorFrame {
  frame: 'error'
  code: string
  message: string
  fatal?: boolean
}

export interface HistoryPageRequestFrame {
  frame: 'history.page'
  request_id: string
  session_id: string
  before_seq?: number
  limit: number
}

export interface HistoryPageResponseFrame {
  frame: 'history.page'
  request_id: string
  session_id: string
  events: AgentWharfEvent[]
  latest_seq: number
  next_before_seq: number | null
  retention_state: string
}

export type AgentWharfFrame =
  | HelloFrame
  | HelloAckFrame
  | AgentWharfEvent
  | CommandFrame
  | CommandAckFrame
  | PingFrame
  | PongFrame
  | ErrorFrame
  | HistoryPageRequestFrame
  | HistoryPageResponseFrame

export interface WebSocketLike {
  onopen: ((event: Event) => void) | null
  onmessage: ((event: MessageEvent<string>) => void) | null
  onerror: ((event: Event) => void) | null
  onclose: ((event: CloseEvent) => void) | null
  send(data: string): void
  close(code?: number, reason?: string): void
}

export type WebSocketFactory = (url: string) => WebSocketLike

export interface ClientSubscription {
  sessionId: string
  lastSeq?: number
}

export interface ReconnectConfig {
  initialDelayMs: number
  maxDelayMs: number
}

export interface AgentWharfClientOptions {
  url: string
  token: string
  sessions: ClientSubscription[]
  webSocketFactory?: WebSocketFactory
  reconnect?: false | Partial<ReconnectConfig>
  commandIdFactory?: () => string
}

export interface SendCommandOptions {
  commandId?: string
}

export interface HistoryPageOptions {
  beforeSeq?: number
  limit?: number
  requestId?: string
  signal?: AbortSignal
}

type EventHandler = (event: AgentWharfEvent) => void
type ErrorHandler = (error: Error | ErrorFrame) => void

interface PendingCommand {
  resolve: (ack: CommandAckFrame) => void
  reject: (error: Error) => void
}

interface PendingHistoryPage {
  resolve: (page: HistoryPageResponseFrame) => void
  reject: (error: Error) => void
  signal?: AbortSignal
  abort: () => void
}

export function encodeFrame(frame: AgentWharfFrame): string {
  return JSON.stringify(frame)
}

export function decodeFrame(data: string): AgentWharfFrame {
  const decoded = JSON.parse(data) as Partial<AgentWharfFrame>
  switch (decoded.frame) {
    case 'hello':
    case 'hello.ack':
    case 'event':
    case 'command':
    case 'command.ack':
    case 'ping':
    case 'pong':
    case 'error':
    case 'history.page':
      return decoded as AgentWharfFrame
    default:
      throw new Error(`unknown frame: ${String(decoded.frame)}`)
  }
}

export class AgentWharfClient {
  private readonly webSocketFactory: WebSocketFactory
  private readonly reconnect: ReconnectConfig | null
  private readonly commandIdFactory: () => string
  private readonly cursors = new Map<string, number>()
  private readonly eventHandlers = new Set<EventHandler>()
  private readonly errorHandlers = new Set<ErrorHandler>()
  private readonly pendingCommands = new Map<string, PendingCommand>()
  private readonly pendingHistoryPages = new Map<string, PendingHistoryPage>()

  private socket: WebSocketLike | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelayMs: number
  private closedByClient = false
  private nextCommandNumber = 1

  constructor(private readonly options: AgentWharfClientOptions) {
    if (options.sessions.length === 0) {
      throw new Error('at least one session subscription is required')
    }
    this.webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory
    this.reconnect = normalizeReconnect(options.reconnect)
    this.reconnectDelayMs = this.reconnect?.initialDelayMs ?? 0
    this.commandIdFactory = options.commandIdFactory ?? (() => `cmd_${Date.now()}_${this.nextCommandNumber++}`)
    for (const session of options.sessions) {
      this.cursors.set(session.sessionId, session.lastSeq ?? 0)
    }
  }

  connect(): Promise<HelloAckFrame> {
    this.closedByClient = false
    return this.openSocket()
  }

  close(): void {
    this.closedByClient = true
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.rejectPendingCommands(new Error('client closed'))
    this.rejectPendingHistoryPages(new Error('client closed'))
    this.socket?.close()
    this.socket = null
  }

  onEvent(handler: EventHandler): () => void {
    this.eventHandlers.add(handler)
    return () => this.eventHandlers.delete(handler)
  }

  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.add(handler)
    return () => this.errorHandlers.delete(handler)
  }

  lastSeq(sessionId: string): number {
    return this.cursors.get(sessionId) ?? 0
  }

  sendMessage(
    sessionId: string,
    content: JsonValue[],
    options: SendCommandOptions = {},
  ): Promise<CommandAckFrame> {
    return this.sendCommand('session.send', sessionId, { content }, options)
  }

  respondPermission(
    sessionId: string,
    payload: JsonObject,
    options: SendCommandOptions = {},
  ): Promise<CommandAckFrame> {
    return this.sendCommand('permission.respond', sessionId, payload, options)
  }

  interrupt(sessionId: string, options: SendCommandOptions = {}): Promise<CommandAckFrame> {
    return this.sendCommand('session.interrupt', sessionId, {}, options)
  }

  stop(sessionId: string, options: SendCommandOptions = {}): Promise<CommandAckFrame> {
    return this.sendCommand('session.stop', sessionId, {}, options)
  }

  sendCommand(
    type: CommandType,
    sessionId: string,
    payload: JsonValue,
    options: SendCommandOptions = {},
  ): Promise<CommandAckFrame> {
    const socket = this.socket
    if (socket === null) {
      return Promise.reject(new Error('client is not connected'))
    }
    const commandId = options.commandId ?? this.commandIdFactory()
    const command: CommandFrame = {
      frame: 'command',
      cmd_id: commandId,
      type,
      session_id: sessionId,
      payload,
    }
    const ack = new Promise<CommandAckFrame>((resolve, reject) => {
      this.pendingCommands.set(commandId, { resolve, reject })
    })
    socket.send(encodeFrame(command))
    return ack
  }

  historyPage(sessionId: string, options: HistoryPageOptions = {}): Promise<HistoryPageResponseFrame> {
    if (!this.cursors.has(sessionId)) {
      return Promise.reject(new Error('session is not subscribed'))
    }
    const socket = this.socket
    if (socket === null) {
      return Promise.reject(new Error('client is not connected'))
    }
    const limit = options.limit ?? 100
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      return Promise.reject(new Error('history page limit must be in 1..100'))
    }
    if (options.beforeSeq !== undefined && (!Number.isInteger(options.beforeSeq) || options.beforeSeq < 1)) {
      return Promise.reject(new Error('history page beforeSeq must be positive'))
    }
    if (options.signal?.aborted) {
      return Promise.reject(new Error('history page request aborted'))
    }
    const requestId = options.requestId ?? `history_${Date.now()}_${this.nextCommandNumber++}`
    const request: HistoryPageRequestFrame = {
      frame: 'history.page', request_id: requestId, session_id: sessionId, limit,
    }
    if (options.beforeSeq !== undefined) request.before_seq = options.beforeSeq
    return new Promise<HistoryPageResponseFrame>((resolve, reject) => {
      const abort = () => {
        if (this.pendingHistoryPages.delete(requestId)) reject(new Error('history page request aborted'))
      }
      this.pendingHistoryPages.set(requestId, { resolve, reject, signal: options.signal, abort })
      options.signal?.addEventListener('abort', abort, { once: true })
      try {
        socket.send(encodeFrame(request))
      } catch (error) {
        this.pendingHistoryPages.delete(requestId)
        options.signal?.removeEventListener('abort', abort)
        reject(normalizeError(error))
      }
    })
  }

  private openSocket(): Promise<HelloAckFrame> {
    const socket = this.webSocketFactory(this.options.url)
    this.socket = socket

    return new Promise<HelloAckFrame>((resolve, reject) => {
      let handshakeComplete = false

      socket.onopen = () => {
        socket.send(encodeFrame(this.helloFrame()))
      }

      socket.onmessage = (event) => {
        try {
          const frame = decodeFrame(event.data)
          if (frame.frame === 'hello.ack') {
            const ack = validateHelloAck(frame)
            handshakeComplete = true
            this.reconnectDelayMs = this.reconnect?.initialDelayMs ?? 0
            resolve(ack)
            return
          }
          this.handleFrame(frame)
        } catch (error) {
          const normalized = normalizeError(error)
          this.emitError(normalized)
          if (!handshakeComplete) {
            if (this.socket === socket) {
              this.socket = null
            }
            socket.onmessage = null
            reject(normalized)
            socket.close()
          }
        }
      }

      socket.onerror = () => {
        const error = new Error('websocket error')
        this.emitError(error)
        if (!handshakeComplete) {
          reject(error)
        }
      }

      socket.onclose = () => {
        if (this.socket !== socket) {
          return
        }
        this.socket = null
        if (!handshakeComplete) {
          reject(new Error('websocket closed before hello.ack'))
        }
        this.rejectPendingCommands(new Error('websocket closed before command.ack'))
        this.rejectPendingHistoryPages(new Error('websocket closed before history.page'))
        if (!this.closedByClient) {
          this.scheduleReconnect()
        }
      }
    })
  }

  private handleFrame(frame: AgentWharfFrame): void {
    switch (frame.frame) {
      case 'event':
        this.handleEvent(frame)
        return
      case 'command.ack':
        this.resolveCommand(frame)
        return
      case 'ping':
        this.socket?.send(encodeFrame({ frame: 'pong', nonce: frame.nonce }))
        return
      case 'error':
        this.emitError(frame)
        return
      case 'history.page':
        if ('events' in frame) this.resolveHistoryPage(frame)
        return
      case 'pong':
      case 'hello':
      case 'hello.ack':
      case 'command':
        return
    }
  }

  private handleEvent(event: AgentWharfEvent): void {
    if (typeof event.seq === 'number') {
      const current = this.cursors.get(event.session_id) ?? 0
      if (event.seq > current) {
        this.cursors.set(event.session_id, event.seq)
      }
    }
    for (const handler of this.eventHandlers) {
      handler(event)
    }
  }

  private resolveCommand(ack: CommandAckFrame): void {
    const pending = this.pendingCommands.get(ack.cmd_id)
    if (pending === undefined) {
      return
    }
    this.pendingCommands.delete(ack.cmd_id)
    pending.resolve(ack)
  }

  private rejectPendingCommands(error: Error): void {
    for (const pending of this.pendingCommands.values()) {
      pending.reject(error)
    }
    this.pendingCommands.clear()
  }

  private rejectPendingHistoryPages(error: Error): void {
    for (const [requestId, pending] of this.pendingHistoryPages) {
      pending.signal?.removeEventListener('abort', pending.abort)
      pending.reject(error)
      this.pendingHistoryPages.delete(requestId)
    }
  }

  private resolveHistoryPage(page: HistoryPageResponseFrame): void {
    const pending = this.pendingHistoryPages.get(page.request_id)
    if (pending === undefined) return
    this.pendingHistoryPages.delete(page.request_id)
    pending.signal?.removeEventListener('abort', pending.abort)
    try {
      validateHistoryPage(page)
      pending.resolve(page)
    } catch (error) {
      pending.reject(normalizeError(error))
    }
  }

  private helloFrame(): HelloFrame {
    return {
      frame: 'hello',
      protocol_version: PROTOCOL_VERSION,
      role: 'client',
      token: this.options.token,
      subscriptions: this.options.sessions.map((session) => ({
        session_id: session.sessionId,
        last_seq: this.lastSeq(session.sessionId),
      })),
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnect === null || this.reconnectTimer !== null) {
      return
    }
    const delay = this.reconnectDelayMs
    this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, this.reconnect.maxDelayMs)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.openSocket().catch((error: unknown) => {
        this.emitError(normalizeError(error))
        this.scheduleReconnect()
      })
    }, delay)
  }

  private emitError(error: Error | ErrorFrame): void {
    for (const handler of this.errorHandlers) {
      handler(error)
    }
  }
}

function validateHistoryPage(page: HistoryPageResponseFrame): void {
  if (page.frame !== 'history.page' || page.request_id === '' || page.session_id === '' || !Array.isArray(page.events)) {
    throw new Error('invalid history.page response')
  }
  if (!Number.isInteger(page.latest_seq) || page.latest_seq < 0 ||
    (page.next_before_seq !== null && (!Number.isInteger(page.next_before_seq) || page.next_before_seq < 1))) {
    throw new Error('invalid history.page cursor')
  }
  let previous = 0
  for (const event of page.events) {
    if (event.frame !== 'event' || event.session_id !== page.session_id || !Number.isInteger(event.seq) ||
      (event.seq as number) <= previous || (event.seq as number) > page.latest_seq) {
      throw new Error('invalid history.page event')
    }
    previous = event.seq as number
  }
}

function defaultWebSocketFactory(url: string): WebSocketLike {
  if (typeof WebSocket === 'undefined') {
    throw new Error('global WebSocket is not available; provide webSocketFactory')
  }
  return new WebSocket(url) as WebSocketLike
}

function normalizeReconnect(reconnect: AgentWharfClientOptions['reconnect']): ReconnectConfig | null {
  if (reconnect === false) {
    return null
  }
  return {
    initialDelayMs: reconnect?.initialDelayMs ?? 250,
    maxDelayMs: reconnect?.maxDelayMs ?? 5000,
  }
}

function normalizeError(error: unknown): Error {
  if (error instanceof Error) {
    return error
  }
  return new Error(String(error))
}

function validateHelloAck(frame: HelloAckFrame): HelloAckFrame {
  if ((frame.protocol_version !== 1 && frame.protocol_version !== 2) || frame.protocol_version > PROTOCOL_VERSION) {
    throw new Error(`unsupported hello.ack protocol version: ${String(frame.protocol_version)}`)
  }
  if (frame.protocol_version === 1 && frame.capabilities !== undefined) {
    throw new Error('v1 hello.ack must omit capabilities')
  }
  return frame
}
