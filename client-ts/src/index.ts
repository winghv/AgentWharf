export const PROTOCOL_VERSION = 2

export type ProtocolVersion = 1 | 2

export type Role = 'client' | 'adapter'
export type CommandType = 'session.send' | 'permission.respond' | 'session.interrupt' | 'session.stop' | 'session.attach' | 'session.settings.change'
export type AckStatus = 'accepted' | 'rejected' | 'duplicate'

/**
 * Routing, Adapter receipt, and Provider completion are separate authorities.
 * A routing acknowledgement alone never advances the latter two states.
 */
export type CommandRoutingState = 'pending' | 'accepted' | 'rejected' | 'duplicate' | 'outcome_unknown'
export type AdapterDeliveryState = 'not_sent' | 'pending' | 'received' | 'outcome_unknown'
export type ProviderCompletionState = 'not_started' | 'pending' | 'completed' | 'rejected' | 'timeout' | 'unsupported' | 'outcome_unknown'

export interface CommandDeliveryState {
  commandId: string
  sessionId: string
  type: CommandType
  routing: CommandRoutingState
  adapter: AdapterDeliveryState
  provider: ProviderCompletionState
  updatedSeq?: number
}

export type AttachState = 'join_pending' | 'queued' | 'start_received' | 'reauthorization_required' | 'canceled'
export type AttachOperation = 'credential_handoff' | 'start' | 'command'

export interface AttachStateUpdate {
  sessionId: string
  attachId?: string
  status: AttachState
  deliveryState: AdapterDeliveryState
  operation?: AttachOperation
  blocker?: string
  version?: number
  updatedSeq?: number
}

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }
export type JsonObject = { [key: string]: JsonValue }

export interface FileReferencePart {
  kind: 'file_reference'
  disposition: 'file' | 'image'
  path: string
  version: string
  contentDigest: string
  bytes: number
  mediaType?: string
}

export interface TextMessagePart {
  kind: 'text'
  text: string
}

export type MessagePart = TextMessagePart | FileReferencePart

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
  settings?: {
    schema_version: number
    max_pending_changes: number
    provider_response_timeout_seconds: number
  }
}

export interface SettingsCapabilityChoice {
  id: string
  label: string
}

export interface SettingsCapability {
  schemaVersion: number
  fingerprint: string
  models: SettingsCapabilityChoice[]
  reasoningEfforts: SettingsCapabilityChoice[]
  permissionModes: SettingsCapabilityChoice[]
  effectiveModelId: string
  effectiveReasoningEffortId?: string
  effectivePermissionModeId: string
  modelChange: 'allowed' | 'read_only'
  reasoningEffortChange: 'allowed' | 'read_only' | 'unsupported'
  permissionChange: 'allowed' | 'read_only'
  modelReadOnlyReason?: string
  reasoningEffortReadOnlyReason?: string
  permissionReadOnlyReason?: string
}

export type SettingsChangeOutcome = 'applied' | 'rejected' | 'timeout' | 'unsupported' | 'stale_capability' | 'outcome_unknown' | 'mismatched_effective'

export interface SettingsEffective {
  commandId: string
  requestFingerprint: string
  effectiveFingerprint: string
  outcome: SettingsChangeOutcome
  effectiveModelId: string
  effectiveReasoningEffortId?: string
  effectivePermissionModeId: string
  reasonCode?: string
}

export interface SettingsCapabilityUpdate {
  sessionId: string
  capability: SettingsCapability
  seq?: number
}

export interface SettingsEffectiveUpdate {
  sessionId: string
  effective: SettingsEffective
  seq?: number
}

export type RunControlOperation = 'interrupt' | 'stop'
export type RunControlOutcome = 'completed' | 'rejected' | 'timeout' | 'unsupported' | 'outcome_unknown'

export interface RunControlCapability {
  schemaVersion: 1
  interruptSupported: boolean
  stopSupported: boolean
}

export interface RunControlOutcomeState {
  commandId: string
  operation: RunControlOperation
  outcome: RunControlOutcome
  completionState?: 'ready' | 'ended'
  reasonCode?: string
}

export interface RunControlCapabilityUpdate {
  sessionId: string
  capability: RunControlCapability
  seq?: number
}

export interface RunControlOutcomeUpdate {
  sessionId: string
  outcome: RunControlOutcomeState
  seq?: number
}

export interface AttentionPermission {
  id: string
  status: 'pending'
}

export interface AttentionBlocker {
  kind: 'queued' | 'reauthorization_required' | 'new_run_required' | 'outcome_unknown'
  reason?: string
  expiresAt?: number
  blockingSessionId?: string
  operation?: string
}

export interface AttentionSummary {
  sessionId: string
  latestSeq: number
  state: string
  permission?: AttentionPermission
  terminalOutcome?: string
  latestChangeSeq?: number
  blocker?: AttentionBlocker
  summaryVersion: number
  summaryState: 'complete' | 'incomplete'
}

export interface AttentionSubscribeFrame {
  frame: 'attention.subscribe'
  request_id: string
}

export interface AttentionSummaryFrame {
  frame: 'attention.summary'
  request_id: string
  kind: 'snapshot' | 'update'
  subscription_state: 'complete' | 'incomplete'
  summaries: AttentionSummaryWire[]
}

interface AttentionSummaryWire {
  session_id: string
  latest_seq: number
  state: string
  permission?: { id: string; status: 'pending' }
  terminal_outcome?: string
  latest_change_seq?: number
  blocker?: {
    kind: AttentionBlocker['kind']
    reason?: string
    expires_at?: number
    blocking_session_id?: string
    operation?: string
  }
  summary_version: number
  summary_state: 'complete' | 'incomplete'
}

export interface AttentionClientOptions {
  url: string
  token: string
  webSocketFactory?: WebSocketFactory
  reconnect?: false | Partial<ReconnectConfig>
  requestIdFactory?: () => string
}

export type AttentionSummaryHandler = (summary: AttentionSummaryFrame) => void

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
  | AttentionSubscribeFrame
  | AttentionSummaryFrame

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
  protocolVersion?: ProtocolVersion
  webSocketFactory?: WebSocketFactory
  reconnect?: false | Partial<ReconnectConfig>
  commandIdFactory?: () => string
}

export interface SendCommandOptions {
  commandId?: string
}

export interface SendMessageWithReferencesOptions extends SendCommandOptions {
  capabilityFingerprint: string
}

export interface AttachCommandOptions extends SendCommandOptions {}

export interface SettingsChangeOptions extends SendCommandOptions {
  capabilityFingerprint: string
  modelId?: string
  reasoningEffortId?: string
  permissionModeId?: string
}

export interface HistoryPageOptions {
  beforeSeq?: number
  limit?: number
  requestId?: string
  signal?: AbortSignal
}

type EventHandler = (event: AgentWharfEvent) => void
type ErrorHandler = (error: Error | ErrorFrame) => void
type DeliveryStateHandler = (state: CommandDeliveryState) => void
type SettingsCapabilityHandler = (update: SettingsCapabilityUpdate) => void
type SettingsEffectiveHandler = (update: SettingsEffectiveUpdate) => void
type RunControlCapabilityHandler = (update: RunControlCapabilityUpdate) => void
type RunControlOutcomeHandler = (update: RunControlOutcomeUpdate) => void

interface PendingCommand {
  resolve: (ack: CommandAckFrame) => void
  reject: (error: Error) => void
  state: CommandDeliveryState
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
    case 'attention.subscribe':
    case 'attention.summary':
      return decoded as AgentWharfFrame
    default:
      throw new Error(`unknown frame: ${String(decoded.frame)}`)
  }
}

export class AgentWharfClient {
  private readonly webSocketFactory: WebSocketFactory
  private readonly protocolVersion: ProtocolVersion
  private readonly reconnect: ReconnectConfig | null
  private readonly commandIdFactory: () => string
  private readonly cursors = new Map<string, number>()
  private readonly eventHandlers = new Set<EventHandler>()
  private readonly errorHandlers = new Set<ErrorHandler>()
  private readonly deliveryStateHandlers = new Set<DeliveryStateHandler>()
  private readonly settingsCapabilityHandlers = new Set<SettingsCapabilityHandler>()
  private readonly settingsEffectiveHandlers = new Set<SettingsEffectiveHandler>()
  private readonly settingsCapabilities = new Map<string, SettingsCapabilityUpdate>()
  private readonly settingsEffectives = new Map<string, SettingsEffectiveUpdate>()
  private readonly runControlCapabilityHandlers = new Set<RunControlCapabilityHandler>()
  private readonly runControlOutcomeHandlers = new Set<RunControlOutcomeHandler>()
  private readonly runControlCapabilities = new Map<string, RunControlCapabilityUpdate>()
  private readonly runControlOutcomes = new Map<string, RunControlOutcomeUpdate>()
  private readonly deliveryStates = new Map<string, CommandDeliveryState>()
  private readonly pendingCommands = new Map<string, PendingCommand>()
  private readonly pendingHistoryPages = new Map<string, PendingHistoryPage>()

  private socket: WebSocketLike | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelayMs: number
  private closedByClient = false
  private nextCommandNumber = 1
  private lastHelloAck: HelloAckFrame | null = null
  private handshakeReady = false

  constructor(private readonly options: AgentWharfClientOptions) {
    if (options.sessions.length === 0) {
      throw new Error('at least one session subscription is required')
    }
    this.webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory
    this.protocolVersion = options.protocolVersion ?? PROTOCOL_VERSION
    this.reconnect = normalizeReconnect(options.reconnect)
    this.reconnectDelayMs = this.reconnect?.initialDelayMs ?? 0
    this.commandIdFactory = options.commandIdFactory ?? (() => `cmd_${Date.now()}_${this.nextCommandNumber++}`)
    for (const session of options.sessions) {
      this.cursors.set(session.sessionId, session.lastSeq ?? 0)
    }
  }

  connect(): Promise<HelloAckFrame> {
    this.closedByClient = false
    this.handshakeReady = false
    return this.openSocket()
  }

  /** Re-open the subscription using the latest durable event cursors. */
  resume(): Promise<HelloAckFrame> {
    this.closedByClient = false
    if (this.socket !== null && this.handshakeReady && this.lastHelloAck !== null) {
      return Promise.resolve(this.lastHelloAck)
    }
    this.handshakeReady = false
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
    this.handshakeReady = false
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

  onDeliveryState(handler: DeliveryStateHandler): () => void {
    this.deliveryStateHandlers.add(handler)
    return () => this.deliveryStateHandlers.delete(handler)
  }

  /** Compatibility alias for callers that use the command-delivery wording. */
  onCommandDelivery(handler: DeliveryStateHandler): () => void {
    return this.onDeliveryState(handler)
  }

  onSettingsCapability(handler: SettingsCapabilityHandler): () => void {
    this.settingsCapabilityHandlers.add(handler)
    return () => this.settingsCapabilityHandlers.delete(handler)
  }

  onSettingsEffective(handler: SettingsEffectiveHandler): () => void {
    this.settingsEffectiveHandlers.add(handler)
    return () => this.settingsEffectiveHandlers.delete(handler)
  }

  settingsCapability(sessionId: string): SettingsCapabilityUpdate | undefined {
    const update = this.settingsCapabilities.get(sessionId)
    return update === undefined ? undefined : cloneSettingsCapabilityUpdate(update)
  }

  settingsEffective(commandId: string): SettingsEffectiveUpdate | undefined {
    const update = this.settingsEffectives.get(commandId)
    return update === undefined ? undefined : cloneSettingsEffectiveUpdate(update)
  }

  onRunControlCapability(handler: RunControlCapabilityHandler): () => void {
    this.runControlCapabilityHandlers.add(handler)
    return () => this.runControlCapabilityHandlers.delete(handler)
  }

  onRunControlOutcome(handler: RunControlOutcomeHandler): () => void {
    this.runControlOutcomeHandlers.add(handler)
    return () => this.runControlOutcomeHandlers.delete(handler)
  }

  runControlCapability(sessionId: string): RunControlCapabilityUpdate | undefined {
    const update = this.runControlCapabilities.get(sessionId)
    return update === undefined ? undefined : { sessionId: update.sessionId, seq: update.seq, capability: { ...update.capability } }
  }

  runControlOutcome(commandId: string): RunControlOutcomeUpdate | undefined {
    const update = this.runControlOutcomes.get(commandId)
    return update === undefined ? undefined : { sessionId: update.sessionId, seq: update.seq, outcome: { ...update.outcome } }
  }

  deliveryState(commandId: string): CommandDeliveryState | undefined {
    const state = this.deliveryStates.get(commandId)
    return state === undefined ? undefined : { ...state }
  }

  commandDeliveryState(commandId: string): CommandDeliveryState | undefined {
    return this.deliveryState(commandId)
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

  sendMessageWithReferences(
    sessionId: string,
    content: MessagePart[],
    options: SendMessageWithReferencesOptions,
  ): Promise<CommandAckFrame> {
    try {
      const payload = encodeFileReferenceMessage(content, options.capabilityFingerprint)
      return this.sendCommand('session.send', sessionId, payload, options)
    } catch (error) {
      return Promise.reject(normalizeError(error))
    }
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

  /**
   * Submit the opaque, in-memory warm-attach grant. The grant is kept in the
   * command payload only; the SDK never decodes, stores, or logs it.
   */
  attach(sessionId: string, grant: string, options: AttachCommandOptions = {}): Promise<CommandAckFrame> {
    if (typeof grant !== 'string' || grant.trim() === '') {
      return Promise.reject(new Error('attach grant is required'))
    }
    return this.sendCommand('session.attach', sessionId, { grant }, options)
  }

  changeSettings(sessionId: string, options: SettingsChangeOptions): Promise<CommandAckFrame> {
    if (!isSettingsFingerprint(options.capabilityFingerprint)) {
      return Promise.reject(new Error('settings capability fingerprint is required'))
    }
    if (options.modelId === undefined && options.reasoningEffortId === undefined && options.permissionModeId === undefined) {
      return Promise.reject(new Error('settings change requires a requested value'))
    }
    if (options.modelId !== undefined && !isSettingsIdentifier(options.modelId)) {
      return Promise.reject(new Error('settings model id is invalid'))
    }
    if (options.reasoningEffortId !== undefined && !isSettingsIdentifier(options.reasoningEffortId)) {
      return Promise.reject(new Error('settings reasoning effort id is invalid'))
    }
    if (options.permissionModeId !== undefined && !isSettingsIdentifier(options.permissionModeId)) {
      return Promise.reject(new Error('settings permission mode id is invalid'))
    }
    const payload: JsonObject = { capability_fingerprint: options.capabilityFingerprint }
    if (options.modelId !== undefined) payload.model_id = options.modelId
    if (options.reasoningEffortId !== undefined) payload.reasoning_effort_id = options.reasoningEffortId
    if (options.permissionModeId !== undefined) payload.permission_mode_id = options.permissionModeId
    return this.sendCommand('session.settings.change', sessionId, payload, options)
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
    if (typeof commandId !== 'string' || commandId.trim() === '') {
      return Promise.reject(new Error('command id is required'))
    }
    if (this.pendingCommands.has(commandId)) {
      return Promise.reject(new Error(`command ${commandId} is already pending`))
    }
    const command: CommandFrame = {
      frame: 'command',
      cmd_id: commandId,
      type,
      session_id: sessionId,
      payload,
    }
    const state: CommandDeliveryState = {
      commandId,
      sessionId,
      type,
      routing: 'pending',
      adapter: 'pending',
      provider: 'pending',
    }
    this.setDeliveryState(state)
    const ack = new Promise<CommandAckFrame>((resolve, reject) => {
      this.pendingCommands.set(commandId, { resolve, reject, state })
    })
    try {
      socket.send(encodeFrame(command))
    } catch (error) {
      this.pendingCommands.delete(commandId)
      this.setDeliveryState({ ...state, routing: 'outcome_unknown', adapter: 'outcome_unknown', provider: 'outcome_unknown' })
      return Promise.reject(normalizeError(error))
    }
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
            const ack = validateHelloAck(frame, this.protocolVersion)
            handshakeComplete = true
            this.handshakeReady = true
            this.lastHelloAck = ack
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
        this.handshakeReady = false
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
      case 'attention.subscribe':
      case 'attention.summary':
        return
    }
  }

  private handleEvent(event: AgentWharfEvent): void {
    this.updateSettingsFromEvent(event)
    this.updateDeliveryFromEvent(event)
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
    const state = this.deliveryStates.get(ack.cmd_id) ?? pending.state
    if (ack.status === 'accepted') {
      this.setDeliveryState({ ...state, routing: 'accepted' })
    } else if (ack.status === 'rejected') {
      this.setDeliveryState({ ...state, routing: 'rejected', adapter: 'not_sent', provider: 'not_started' })
    } else {
      // A duplicate proves only that Hub has seen this command before. The
      // original Adapter/Provider outcome still requires durable evidence.
      this.setDeliveryState({ ...state, routing: 'duplicate', adapter: 'outcome_unknown', provider: 'outcome_unknown' })
    }
    pending.resolve(ack)
  }

  private rejectPendingCommands(error: Error): void {
    for (const pending of this.pendingCommands.values()) {
      const state = this.deliveryStates.get(pending.state.commandId) ?? pending.state
      this.setDeliveryState({
        ...state,
        routing: 'outcome_unknown',
        adapter: state.adapter === 'received' ? state.adapter : 'outcome_unknown',
        provider: state.provider === 'completed' || state.provider === 'rejected' || state.provider === 'timeout' || state.provider === 'unsupported'
          ? state.provider : 'outcome_unknown',
      })
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
      protocol_version: this.protocolVersion,
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

  private setDeliveryState(state: CommandDeliveryState): void {
    const previous = this.deliveryStates.get(state.commandId)
    if (previous !== undefined && previous.updatedSeq !== undefined && state.updatedSeq !== undefined && state.updatedSeq < previous.updatedSeq) {
      return
    }
    this.deliveryStates.set(state.commandId, state)
    for (const handler of this.deliveryStateHandlers) {
      handler({ ...state })
    }
  }

  private updateDeliveryFromEvent(event: AgentWharfEvent): void {
    const payload = asJsonObject(event.payload)
    if (payload === null) return
    const commandId = stringField(payload, 'cmd_id') ?? stringField(payload, 'command_id')
    if (commandId === undefined) return
    const state = this.deliveryStates.get(commandId)
    if (state === undefined || event.session_id !== state.sessionId) return
    const next = { ...state, updatedSeq: event.seq ?? state.updatedSeq }
    const deliveryState = stringField(payload, 'delivery_state')
    const status = stringField(payload, 'status')
    const outcome = stringField(payload, 'outcome')
    if (deliveryState === 'received') next.adapter = 'received'
    if (deliveryState === 'outcome_unknown') next.adapter = 'outcome_unknown'
    if (status === 'received') next.adapter = 'received'
    if (status === 'outcome_unknown') next.adapter = 'outcome_unknown'
    switch (outcome) {
      case 'completed': next.provider = 'completed'; break
      case 'rejected': next.provider = 'rejected'; break
      case 'timeout': next.provider = 'timeout'; break
      case 'unsupported': next.provider = 'unsupported'; break
      case 'outcome_unknown': next.provider = 'outcome_unknown'; break
    }
    if (next.adapter !== state.adapter || next.provider !== state.provider || next.updatedSeq !== state.updatedSeq) {
      this.setDeliveryState(next)
    }
  }

  private updateSettingsFromEvent(event: AgentWharfEvent): void {
    if (event.type === 'session.settings.capabilities') {
      const capability = decodeSettingsCapability(event.payload)
      const previous = this.settingsCapabilities.get(event.session_id)
      if (previous?.seq !== undefined && event.seq !== undefined && event.seq < previous.seq) return
      const update: SettingsCapabilityUpdate = { sessionId: event.session_id, capability, seq: event.seq }
      this.settingsCapabilities.set(event.session_id, update)
      for (const handler of this.settingsCapabilityHandlers) handler(cloneSettingsCapabilityUpdate(update))
      return
    }
    if (event.type === 'session.settings.effective') {
      const effective = decodeSettingsEffective(event.payload)
      const previous = this.settingsEffectives.get(effective.commandId)
      if (previous?.seq !== undefined && event.seq !== undefined && event.seq < previous.seq) return
      const update: SettingsEffectiveUpdate = { sessionId: event.session_id, effective, seq: event.seq }
      this.settingsEffectives.set(effective.commandId, update)
      for (const handler of this.settingsEffectiveHandlers) handler(cloneSettingsEffectiveUpdate(update))
      return
    }
    if (event.type === 'session.run.capabilities') {
      const capability = decodeRunControlCapability(event.payload)
      const previous = this.runControlCapabilities.get(event.session_id)
      if (previous?.seq !== undefined && event.seq !== undefined && event.seq < previous.seq) return
      const update: RunControlCapabilityUpdate = { sessionId: event.session_id, capability, seq: event.seq }
      this.runControlCapabilities.set(event.session_id, update)
      for (const handler of this.runControlCapabilityHandlers) handler({ sessionId: update.sessionId, seq: update.seq, capability: { ...update.capability } })
      return
    }
    if (event.type === 'session.run.outcome') {
      const outcome = decodeRunControlOutcome(event.payload)
      const previous = this.runControlOutcomes.get(outcome.commandId)
      if (previous?.seq !== undefined && event.seq !== undefined && event.seq < previous.seq) return
      const update: RunControlOutcomeUpdate = { sessionId: event.session_id, outcome, seq: event.seq }
      this.runControlOutcomes.set(outcome.commandId, update)
      for (const handler of this.runControlOutcomeHandlers) handler({ sessionId: update.sessionId, seq: update.seq, outcome: { ...update.outcome } })
    }
  }
}

/**
 * Attention-only client. It deliberately has no command, attach, history or
 * credential APIs; membership and summaries come solely from the Auth grant.
 */
export class AgentWharfAttentionClient {
  private readonly webSocketFactory: WebSocketFactory
  private readonly reconnect: ReconnectConfig | null
  private readonly requestIdFactory: () => string
  private readonly summaryHandlers = new Set<AttentionSummaryHandler>()
  private readonly summariesBySession = new Map<string, AttentionSummary>()
  private readonly pendingRefresh = new Map<string, { resolve: (frame: AttentionSummaryFrame) => void; reject: (error: Error) => void }>()
  private token: string
  private socket: WebSocketLike | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelayMs: number
  private closedByClient = false
  private handshakeReady = false
  private subscriptionState: 'complete' | 'incomplete' = 'incomplete'
  private nextRequestNumber = 1

  constructor(private readonly options: AttentionClientOptions) {
    if (options.token.trim() === '') throw new Error('attention token is required')
    this.token = options.token
    this.webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory
    this.reconnect = normalizeReconnect(options.reconnect)
    this.reconnectDelayMs = this.reconnect?.initialDelayMs ?? 0
    this.requestIdFactory = options.requestIdFactory ?? (() => `attention_${Date.now()}_${this.nextRequestNumber++}`)
  }

  connect(): Promise<AttentionSummaryFrame> {
    this.closedByClient = false
    this.handshakeReady = false
    return this.openSocket().then(() => this.refresh())
  }

  resume(): Promise<AttentionSummaryFrame> {
    this.closedByClient = false
    if (this.socket !== null && this.handshakeReady) return this.refresh()
    this.handshakeReady = false
    return this.openSocket().then(() => this.refresh())
  }

  switchIdentity(token: string): Promise<AttentionSummaryFrame> {
    if (typeof token !== 'string' || token.trim() === '') return Promise.reject(new Error('attention token is required'))
    this.token = token
    this.summariesBySession.clear()
    this.subscriptionState = 'incomplete'
    this.close()
    return this.connect()
  }

  close(): void {
    this.closedByClient = true
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.rejectPending(new Error('attention client closed'))
    this.handshakeReady = false
    this.socket?.close()
    this.socket = null
  }

  onSummary(handler: AttentionSummaryHandler): () => void {
    this.summaryHandlers.add(handler)
    return () => this.summaryHandlers.delete(handler)
  }

  summary(sessionId: string): AttentionSummary | undefined {
    const summary = this.summariesBySession.get(sessionId)
    return summary === undefined ? undefined : cloneAttentionSummary(summary)
  }

  summaries(): AttentionSummary[] {
    return [...this.summariesBySession.values()].map(cloneAttentionSummary)
  }

  currentSubscriptionState(): 'complete' | 'incomplete' {
    return this.subscriptionState
  }

  refresh(): Promise<AttentionSummaryFrame> {
    const socket = this.socket
    if (socket === null || !this.handshakeReady) return Promise.reject(new Error('attention client is not connected'))
    const requestId = this.requestIdFactory()
    if (!isSettingsIdentifier(requestId) || this.pendingRefresh.has(requestId)) return Promise.reject(new Error('attention request id is invalid or pending'))
    const frame: AttentionSubscribeFrame = { frame: 'attention.subscribe', request_id: requestId }
    return new Promise<AttentionSummaryFrame>((resolve, reject) => {
      this.pendingRefresh.set(requestId, { resolve, reject })
      try {
        socket.send(encodeFrame(frame))
      } catch (error) {
        this.pendingRefresh.delete(requestId)
        reject(normalizeError(error))
      }
    })
  }

  private openSocket(): Promise<void> {
    const socket = this.webSocketFactory(this.options.url)
    this.socket = socket
    return new Promise<void>((resolve, reject) => {
      let handshakeComplete = false
      socket.onopen = () => {
        socket.send(encodeFrame({
          frame: 'hello', protocol_version: 2, role: 'client', token: this.token, subscriptions: [],
        } satisfies HelloFrame))
      }
      socket.onmessage = (event) => {
        try {
          const frame = decodeFrame(event.data)
          if (frame.frame === 'hello.ack') {
            validateHelloAck(frame, 2)
            handshakeComplete = true
            this.handshakeReady = true
            this.reconnectDelayMs = this.reconnect?.initialDelayMs ?? 0
            resolve()
            return
          }
          if (frame.frame === 'attention.summary') {
            validateAttentionSummaryFrame(frame)
            this.applyAttentionSummary(frame)
            const pending = this.pendingRefresh.get(frame.request_id)
            if (pending !== undefined) {
              this.pendingRefresh.delete(frame.request_id)
              pending.resolve(frame)
            }
            for (const handler of this.summaryHandlers) handler(frame)
            return
          }
          if (frame.frame === 'ping') {
            socket.send(encodeFrame({ frame: 'pong', nonce: frame.nonce }))
          }
        } catch (error) {
          const normalized = normalizeError(error)
          if (!handshakeComplete) {
            if (this.socket === socket) this.socket = null
            reject(normalized)
            socket.close()
          } else {
            this.rejectPending(normalized)
            socket.close()
          }
        }
      }
      socket.onerror = () => {
        const error = new Error('websocket error')
        if (!handshakeComplete) reject(error)
      }
      socket.onclose = () => {
        if (this.socket !== socket) return
        this.socket = null
        this.handshakeReady = false
        if (!handshakeComplete) reject(new Error('websocket closed before hello.ack'))
        this.rejectPending(new Error('attention websocket closed'))
        if (!this.closedByClient) this.scheduleReconnect()
      }
    })
  }

  private applyAttentionSummary(frame: AttentionSummaryFrame): void {
    if (frame.kind === 'snapshot') this.summariesBySession.clear()
    this.subscriptionState = frame.subscription_state
    for (const wire of frame.summaries) {
      const summary = attentionSummaryFromWire(wire)
      const previous = this.summariesBySession.get(summary.sessionId)
      if (previous !== undefined && (summary.latestSeq < previous.latestSeq || summary.summaryVersion < previous.summaryVersion)) continue
      this.summariesBySession.set(summary.sessionId, summary)
    }
  }

  private rejectPending(error: Error): void {
    for (const pending of this.pendingRefresh.values()) pending.reject(error)
    this.pendingRefresh.clear()
  }

  private scheduleReconnect(): void {
    if (this.reconnect === null || this.reconnectTimer !== null) return
    const delay = this.reconnectDelayMs
    this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, this.reconnect.maxDelayMs)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.openSocket().then(() => this.refresh()).catch(() => this.scheduleReconnect())
    }, delay)
  }
}

function asJsonObject(value: JsonValue): { [key: string]: JsonValue } | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as { [key: string]: JsonValue } : null
}

function stringField(value: { [key: string]: JsonValue }, key: string): string | undefined {
  const field = value[key]
  return typeof field === 'string' ? field : undefined
}

function isSettingsFingerprint(value: string): boolean {
  return /^sha256:[0-9a-f]{64}$/.test(value)
}

function isSettingsIdentifier(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(value)
}

function encodeFileReferenceMessage(content: MessagePart[], capabilityFingerprint: string): JsonObject {
  if (!isSettingsFingerprint(capabilityFingerprint) || content.length === 0 || content.length > 32) {
    throw new Error('file-reference capability or content is invalid')
  }
  let references = 0
  const encoded = content.map<JsonObject>((part) => {
    if (part.kind === 'text') {
      if (typeof part.text !== 'string') throw new Error('text message part is invalid')
      return { kind: 'text', text: part.text } as JsonObject
    }
    if (part.kind !== 'file_reference' || !isFileReferencePath(part.path) || !isFileReferenceVersion(part.version) ||
      (part.disposition !== 'file' && part.disposition !== 'image') || !isFileReferenceFingerprint(part.contentDigest) ||
      !Number.isInteger(part.bytes) || part.bytes < 0 || part.bytes > 10 * 1024 * 1024 || !isFileReferenceMediaType(part.mediaType)) {
      throw new Error('file-reference message part is invalid')
    }
    references += 1
    if (references > 8) throw new Error('too many file references')
    return {
      kind: 'file_reference', disposition: part.disposition, path: part.path, version: part.version,
      content_digest: part.contentDigest, bytes: part.bytes, media_type: part.mediaType ?? null,
    } as JsonObject
  })
  if (references === 0) throw new Error('at least one file reference is required')
  const payload: JsonObject = { content: encoded, capability_fingerprint: capabilityFingerprint }
  if (utf8ByteLength(JSON.stringify(payload)) > 8192) throw new Error('file-reference message is too large')
  return payload
}

function isFileReferencePath(value: string): boolean {
  if (value.length < 1 || value.length > 1024 || value.startsWith('/') || value.includes('\\') || value.includes('\u0000')) return false
  const parts = value.split('/')
  return parts.length <= 32 && parts.every((part) => part.length > 0 && part !== '.' && part !== '..')
}

function isFileReferenceVersion(value: string): boolean {
  return value.length >= 1 && value.length <= 256 && !value.includes('\u0000')
}

function isFileReferenceFingerprint(value: string): boolean {
  return isSettingsFingerprint(value)
}

function isFileReferenceMediaType(value: string | undefined): boolean {
  return value === undefined || (value.length >= 1 && value.length <= 127 && /^[\x20-\x7e]+$/.test(value))
}

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength
}

function validateAttentionSummaryFrame(frame: AttentionSummaryFrame): void {
  if (!isSettingsIdentifier(frame.request_id) || (frame.kind !== 'snapshot' && frame.kind !== 'update') ||
    (frame.subscription_state !== 'complete' && frame.subscription_state !== 'incomplete') ||
    !Array.isArray(frame.summaries) || frame.summaries.length > 64) {
    throw new Error('invalid attention summary')
  }
  const seen = new Set<string>()
  for (const summary of frame.summaries) {
    if (!isSettingsIdentifier(summary.session_id) || seen.has(summary.session_id) ||
      !Number.isInteger(summary.latest_seq) || summary.latest_seq < 0 || typeof summary.state !== 'string' || summary.state.length === 0 ||
      !Number.isInteger(summary.summary_version) || summary.summary_version < 0 ||
      (summary.summary_state !== 'complete' && summary.summary_state !== 'incomplete')) {
      throw new Error('invalid attention summary')
    }
    seen.add(summary.session_id)
    if (summary.permission !== undefined && (summary.permission.status !== 'pending' || !isSettingsIdentifier(summary.permission.id))) {
      throw new Error('invalid attention summary permission')
    }
    if (summary.terminal_outcome !== undefined && typeof summary.terminal_outcome !== 'string') {
      throw new Error('invalid attention summary terminal outcome')
    }
    if (summary.latest_change_seq !== undefined && (!Number.isInteger(summary.latest_change_seq) || summary.latest_change_seq < 0)) {
      throw new Error('invalid attention summary change sequence')
    }
    const blocker = summary.blocker
    if (blocker === undefined) continue
    if (!['queued', 'reauthorization_required', 'new_run_required', 'outcome_unknown'].includes(blocker.kind)) {
      throw new Error('invalid attention summary blocker')
    }
    if (blocker.reason !== undefined && typeof blocker.reason !== 'string') throw new Error('invalid attention summary blocker reason')
    if (blocker.expires_at !== undefined && (!Number.isInteger(blocker.expires_at) || blocker.expires_at < 0)) throw new Error('invalid attention summary blocker expiry')
    if (blocker.blocking_session_id !== undefined && !isSettingsIdentifier(blocker.blocking_session_id)) throw new Error('invalid attention summary blocker session')
    if (blocker.operation !== undefined && typeof blocker.operation !== 'string') throw new Error('invalid attention summary blocker operation')
    if (blocker.kind !== 'queued' && (blocker.reason !== undefined || blocker.expires_at !== undefined || blocker.blocking_session_id !== undefined)) {
      throw new Error('invalid attention summary blocker fields')
    }
    if (blocker.kind !== 'outcome_unknown' && blocker.operation !== undefined) throw new Error('invalid attention summary blocker operation')
  }
}

function attentionSummaryFromWire(wire: AttentionSummaryWire): AttentionSummary {
  return {
    sessionId: wire.session_id,
    latestSeq: wire.latest_seq,
    state: wire.state,
    ...(wire.permission === undefined ? {} : { permission: { ...wire.permission } }),
    ...(wire.terminal_outcome === undefined ? {} : { terminalOutcome: wire.terminal_outcome }),
    ...(wire.latest_change_seq === undefined ? {} : { latestChangeSeq: wire.latest_change_seq }),
    ...(wire.blocker === undefined ? {} : {
      blocker: {
        kind: wire.blocker.kind,
        ...(wire.blocker.reason === undefined ? {} : { reason: wire.blocker.reason }),
        ...(wire.blocker.expires_at === undefined ? {} : { expiresAt: wire.blocker.expires_at }),
        ...(wire.blocker.blocking_session_id === undefined ? {} : { blockingSessionId: wire.blocker.blocking_session_id }),
        ...(wire.blocker.operation === undefined ? {} : { operation: wire.blocker.operation }),
      },
    }),
    summaryVersion: wire.summary_version,
    summaryState: wire.summary_state,
  }
}

function cloneAttentionSummary(summary: AttentionSummary): AttentionSummary {
  return {
    ...summary,
    ...(summary.permission === undefined ? {} : { permission: { ...summary.permission } }),
    ...(summary.blocker === undefined ? {} : { blocker: { ...summary.blocker } }),
  }
}

function decodeSettingsCapability(value: JsonValue): SettingsCapability {
  const object = asJsonObject(value)
  if (object === null || (object.schema_version !== 1 && object.schema_version !== 2) || typeof object.fingerprint !== 'string' ||
    !isSettingsFingerprint(object.fingerprint) || !Array.isArray(object.models) || !Array.isArray(object.permission_modes) ||
    typeof object.effective_model_id !== 'string' || typeof object.effective_permission_mode_id !== 'string' ||
    (object.model_change !== 'allowed' && object.model_change !== 'read_only') ||
    (object.permission_change !== 'allowed' && object.permission_change !== 'read_only')) {
    throw new Error('invalid settings capability event')
  }
  const models = decodeSettingsChoices(object.models, 32)
  const permissionModes = decodeSettingsChoices(object.permission_modes, 16)
  if (!isSettingsIdentifier(object.effective_model_id) || !isSettingsIdentifier(object.effective_permission_mode_id) ||
    !models.some((choice) => choice.id === object.effective_model_id) ||
    !permissionModes.some((choice) => choice.id === object.effective_permission_mode_id)) {
    throw new Error('invalid settings capability event')
  }
  const modelReadOnlyReason = optionalSettingsReason(object.model_read_only_reason)
  const permissionReadOnlyReason = optionalSettingsReason(object.permission_read_only_reason)
  if (object.model_change === 'read_only' && modelReadOnlyReason === undefined ||
    object.model_change === 'allowed' && modelReadOnlyReason !== undefined ||
    object.permission_change === 'read_only' && permissionReadOnlyReason === undefined ||
    object.permission_change === 'allowed' && permissionReadOnlyReason !== undefined) {
    throw new Error('invalid settings capability event')
  }
  if (object.schema_version === 1) {
    return {
      schemaVersion: 1,
      fingerprint: object.fingerprint,
      models,
      reasoningEfforts: [],
      permissionModes,
      effectiveModelId: object.effective_model_id,
      effectivePermissionModeId: object.effective_permission_mode_id,
      modelChange: object.model_change,
      reasoningEffortChange: 'unsupported',
      permissionChange: object.permission_change,
      ...(modelReadOnlyReason === undefined ? {} : { modelReadOnlyReason }),
      reasoningEffortReadOnlyReason: 'provider_unsupported',
      ...(permissionReadOnlyReason === undefined ? {} : { permissionReadOnlyReason }),
    }
  }
  if (!Array.isArray(object.reasoning_efforts) ||
    (object.reasoning_effort_change !== 'allowed' && object.reasoning_effort_change !== 'read_only' && object.reasoning_effort_change !== 'unsupported')) {
    throw new Error('invalid settings capability event')
  }
  const reasoningEfforts = object.reasoning_efforts.length === 0 ? [] : decodeSettingsChoices(object.reasoning_efforts, 16)
  const effectiveReasoningEffortId = object.effective_reasoning_effort_id === null ? undefined : object.effective_reasoning_effort_id
  const reasoningEffortReadOnlyReason = optionalSettingsReason(object.reasoning_effort_read_only_reason)
  if (object.reasoning_effort_change === 'unsupported') {
    if (reasoningEfforts.length !== 0 || effectiveReasoningEffortId !== undefined || reasoningEffortReadOnlyReason === undefined) {
      throw new Error('invalid settings capability event')
    }
  } else if (typeof effectiveReasoningEffortId !== 'string' || !isSettingsIdentifier(effectiveReasoningEffortId) ||
    !reasoningEfforts.some((choice) => choice.id === effectiveReasoningEffortId) ||
    object.reasoning_effort_change === 'allowed' && (reasoningEfforts.length < 2 || reasoningEffortReadOnlyReason !== undefined) ||
    object.reasoning_effort_change === 'read_only' && reasoningEffortReadOnlyReason === undefined) {
    throw new Error('invalid settings capability event')
  }
  return {
    schemaVersion: object.schema_version,
    fingerprint: object.fingerprint,
    models,
    reasoningEfforts,
    permissionModes,
    effectiveModelId: object.effective_model_id,
    ...(effectiveReasoningEffortId === undefined ? {} : { effectiveReasoningEffortId }),
    effectivePermissionModeId: object.effective_permission_mode_id,
    modelChange: object.model_change,
    reasoningEffortChange: object.reasoning_effort_change,
    permissionChange: object.permission_change,
    ...(modelReadOnlyReason === undefined ? {} : { modelReadOnlyReason }),
    ...(reasoningEffortReadOnlyReason === undefined ? {} : { reasoningEffortReadOnlyReason }),
    ...(permissionReadOnlyReason === undefined ? {} : { permissionReadOnlyReason }),
  }
}

function decodeSettingsChoices(value: JsonValue[], maximum: number): SettingsCapabilityChoice[] {
  if (value.length < 1 || value.length > maximum) throw new Error('invalid settings capability choices')
  return value.map((entry) => {
    const object = asJsonObject(entry)
    if (object === null || typeof object.id !== 'string' || typeof object.label !== 'string' ||
      !isSettingsIdentifier(object.id) || object.label.length === 0 || object.label.length > 128) {
      throw new Error('invalid settings capability choices')
    }
    return { id: object.id, label: object.label }
  })
}

function optionalSettingsReason(value: JsonValue | undefined): string | undefined {
  if (value === undefined || value === null) return undefined
  return typeof value === 'string' && isSettingsIdentifier(value) ? value : (() => { throw new Error('invalid settings capability reason') })()
}

function decodeSettingsEffective(value: JsonValue): SettingsEffective {
  const object = asJsonObject(value)
  const outcome = object?.outcome
  if (object === null || typeof object.cmd_id !== 'string' || !isSettingsIdentifier(object.cmd_id) ||
    typeof object.request_fingerprint !== 'string' || !isSettingsFingerprint(object.request_fingerprint) ||
    typeof object.effective_fingerprint !== 'string' || !isSettingsFingerprint(object.effective_fingerprint) ||
    typeof outcome !== 'string' || !['applied', 'rejected', 'timeout', 'unsupported', 'stale_capability', 'outcome_unknown', 'mismatched_effective'].includes(outcome) ||
    typeof object.effective_model_id !== 'string' || !isSettingsIdentifier(object.effective_model_id) ||
    typeof object.effective_permission_mode_id !== 'string' || !isSettingsIdentifier(object.effective_permission_mode_id)) {
    throw new Error('invalid settings effective event')
  }
  const reasonCode = object.reason_code === undefined || object.reason_code === null ? undefined : object.reason_code
  if (reasonCode !== undefined && (typeof reasonCode !== 'string' || !isSettingsIdentifier(reasonCode))) {
    throw new Error('invalid settings effective event')
  }
  const effectiveReasoningEffortId = object.effective_reasoning_effort_id === undefined || object.effective_reasoning_effort_id === null
    ? undefined
    : object.effective_reasoning_effort_id
  if (effectiveReasoningEffortId !== undefined && (typeof effectiveReasoningEffortId !== 'string' || !isSettingsIdentifier(effectiveReasoningEffortId))) {
    throw new Error('invalid settings effective event')
  }
  return {
    commandId: object.cmd_id,
    requestFingerprint: object.request_fingerprint,
    effectiveFingerprint: object.effective_fingerprint,
    outcome: outcome as SettingsChangeOutcome,
    effectiveModelId: object.effective_model_id,
    ...(effectiveReasoningEffortId === undefined ? {} : { effectiveReasoningEffortId }),
    effectivePermissionModeId: object.effective_permission_mode_id,
    ...(reasonCode === undefined ? {} : { reasonCode }),
  }
}

function decodeRunControlCapability(value: JsonValue): RunControlCapability {
  const object = asJsonObject(value)
  if (object === null || object.schema_version !== 1 || typeof object.interrupt_supported !== 'boolean' || typeof object.stop_supported !== 'boolean') {
    throw new Error('invalid run-control capability event')
  }
  return { schemaVersion: 1, interruptSupported: object.interrupt_supported, stopSupported: object.stop_supported }
}

function decodeRunControlOutcome(value: JsonValue): RunControlOutcomeState {
  const object = asJsonObject(value)
  const operation = object?.operation
  const outcome = object?.outcome
  if (object === null || typeof object.cmd_id !== 'string' || !isSettingsIdentifier(object.cmd_id) ||
    (operation !== 'interrupt' && operation !== 'stop') ||
    (outcome !== 'completed' && outcome !== 'rejected' && outcome !== 'timeout' && outcome !== 'unsupported' && outcome !== 'outcome_unknown')) {
    throw new Error('invalid run-control outcome event')
  }
  const completionState = object.completion_state === null || object.completion_state === undefined ? undefined : object.completion_state
  const reasonCode = object.reason_code === null || object.reason_code === undefined ? undefined : object.reason_code
  if (completionState !== undefined && completionState !== 'ready' && completionState !== 'ended') {
    throw new Error('invalid run-control outcome event')
  }
  if (reasonCode !== undefined && (typeof reasonCode !== 'string' || !isSettingsIdentifier(reasonCode))) {
    throw new Error('invalid run-control outcome event')
  }
  if (outcome === 'completed' && (completionState !== (operation === 'stop' ? 'ended' : 'ready') || reasonCode !== undefined)) {
    throw new Error('invalid run-control outcome event')
  }
  if (outcome !== 'completed' && completionState !== undefined) {
    throw new Error('invalid run-control outcome event')
  }
  return {
    commandId: object.cmd_id,
    operation,
    outcome,
    ...(completionState === undefined ? {} : { completionState }),
    ...(reasonCode === undefined ? {} : { reasonCode }),
  }
}

function cloneSettingsCapabilityUpdate(update: SettingsCapabilityUpdate): SettingsCapabilityUpdate {
  return { sessionId: update.sessionId, seq: update.seq, capability: { ...update.capability, models: update.capability.models.map((choice) => ({ ...choice })), reasoningEfforts: update.capability.reasoningEfforts.map((choice) => ({ ...choice })), permissionModes: update.capability.permissionModes.map((choice) => ({ ...choice })) } }
}

function cloneSettingsEffectiveUpdate(update: SettingsEffectiveUpdate): SettingsEffectiveUpdate {
  return { sessionId: update.sessionId, seq: update.seq, effective: { ...update.effective } }
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

function validateHelloAck(frame: HelloAckFrame, requestedVersion: ProtocolVersion): HelloAckFrame {
  if ((frame.protocol_version !== 1 && frame.protocol_version !== 2) || frame.protocol_version > requestedVersion) {
    throw new Error(`unsupported hello.ack protocol version: ${String(frame.protocol_version)}`)
  }
  if (frame.protocol_version === 1 && frame.capabilities !== undefined) {
    throw new Error('v1 hello.ack must omit capabilities')
  }
  return frame
}
