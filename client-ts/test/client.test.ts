import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  AgentWharfClient,
  AgentWharfAttentionClient,
  decodeFrame,
  encodeFrame,
  type AgentWharfEvent,
  type CommandAckFrame,
  type HelloFrame,
  type WebSocketFactory,
} from '../src/index.js'

test('encodes and decodes protocol frames', () => {
  const hello: HelloFrame = {
    frame: 'hello',
    protocol_version: 2,
    role: 'client',
    token: 'control-token',
    subscriptions: [{ session_id: 'ses_1', last_seq: 7 }],
  }

  const encoded = encodeFrame(hello)
  assert.equal(JSON.parse(encoded).frame, 'hello')
  assert.deepEqual(decodeFrame(encoded), hello)
  assert.throws(() => decodeFrame('{"frame":"unknown"}'), /unknown frame/)
})

test('connect sends client hello with the current replay cursor', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws',
    token: 'control-token',
    sessions: [{ sessionId: 'ses_1', lastSeq: 4 }],
    webSocketFactory: sockets.factory,
    reconnect: false,
  })

  const ackPromise = client.connect()
  const socket = sockets.last()
  socket.open()

  assert.deepEqual(socket.sentFrames()[0], {
    frame: 'hello',
    protocol_version: 2,
    role: 'client',
    token: 'control-token',
    subscriptions: [{ session_id: 'ses_1', last_seq: 4 }],
  })

  socket.receive({
    frame: 'hello.ack',
    protocol_version: 1,
    sessions: [
      { session_id: 'ses_1', state: 'ready', provider: 'claude-code', latest_seq: 4, replay_from: 5 },
    ],
  })

  const ack = await ackPromise
  assert.equal(ack.protocol_version, 1)
  assert.equal(ack.sessions[0]?.replay_from, 5)
  client.close()
})

test('requests typed reverse history pages and validates cursors', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'view-token', sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await connected

  const pagePromise = client.historyPage('ses_1', { beforeSeq: 8, limit: 2, requestId: 'history_1' })
  assert.deepEqual(sockets.last().sentFrames()[1], {
    frame: 'history.page', request_id: 'history_1', session_id: 'ses_1', before_seq: 8, limit: 2,
  })
  sockets.last().receive({
    frame: 'history.page', request_id: 'history_1', session_id: 'ses_1',
    events: [
      { frame: 'event', type: 'session.message', session_id: 'ses_1', seq: 5, time: 5, payload: {} },
      { frame: 'event', type: 'session.message', session_id: 'ses_1', seq: 7, time: 7, payload: {} },
    ], latest_seq: 7, next_before_seq: 5, retention_state: 'complete',
  })
  const page = await pagePromise
  assert.equal(page.events.length, 2)
  assert.equal(page.next_before_seq, 5)
  client.close()
})

test('cancels pending history pages on abort and reconnect close', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'view-token', sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await connected
  const controller = new AbortController()
  const aborted = client.historyPage('ses_1', { signal: controller.signal })
  controller.abort()
  await assert.rejects(aborted, /aborted/)
  const closed = client.historyPage('ses_1')
  sockets.last().serverClose()
  await assert.rejects(closed, /websocket closed before history\.page/)
  client.close()
})

test('rejects capabilities on a v1 acknowledgement', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory, reconnect: { initialDelayMs: 1, maxDelayMs: 1 },
  })
  const seen: AgentWharfEvent[] = []
  client.onEvent((event) => seen.push(event))
  const ackPromise = client.connect()
  sockets.last().open()
  sockets.last().receive({
    frame: 'hello.ack', protocol_version: 1, sessions: [],
    capabilities: { history_page: { max_limit: 100 } },
  })
  await assert.rejects(ackPromise, /v1 hello\.ack must omit capabilities/)
  assert.equal(sockets.last().isClosed(), true)
  await assert.rejects(client.sendMessage('ses_1', []), /client is not connected/)
  sockets.last().receive({frame: 'hello.ack', protocol_version: 2, sessions: []})
  sockets.last().receive({
    frame: 'event', type: 'session.message', session_id: 'ses_1', seq: 1, time: 1, payload: {},
  })
  assert.deepEqual(seen, [])
  await new Promise((resolve) => setTimeout(resolve, 10))
  assert.equal(sockets.all.length, 1)
})

test('stale protocol-failure close cannot reject a new connection command', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const rejected = client.connect()
  const stale = sockets.last()
  stale.deferCloseEvent()
  stale.open()
  stale.receive({frame: 'hello.ack', protocol_version: 3, sessions: []})
  await assert.rejects(rejected, /unsupported hello\.ack protocol version/)

  const connected = client.connect()
  const current = sockets.last()
  current.open()
  current.receive({frame: 'hello.ack', protocol_version: 1, sessions: []})
  await connected
  const command = client.sendMessage('ses_1', [], {commandId: 'cmd_after_reconnect'})
  stale.finishClose()
  current.receive({frame: 'command.ack', cmd_id: 'cmd_after_reconnect', status: 'accepted', reason: ''})
  assert.equal((await command).status, 'accepted')
  client.close()
})

test('tracks durable event sequence for reconnect replay', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws',
    token: 'control-token',
    sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory,
    reconnect: { initialDelayMs: 1, maxDelayMs: 1 },
  })

  const ackPromise = client.connect()
  sockets.last().open()
  sockets.last().receive({
    frame: 'hello.ack',
    protocol_version: 1,
    sessions: [
      { session_id: 'ses_1', state: 'ready', provider: 'claude-code', latest_seq: 0, replay_from: 1 },
    ],
  })
  await ackPromise

  sockets.last().receive({
    frame: 'event',
    type: 'session.message',
    session_id: 'ses_1',
    seq: 7,
    time: 1764937200123,
    payload: { role: 'agent', content: [{ kind: 'text', text: 'pong' }] },
  })

  sockets.last().serverClose()
  await waitFor(() => sockets.all.length === 2)
  sockets.last().open()

  assert.deepEqual(sockets.last().sentFrames()[0].subscriptions, [{ session_id: 'ses_1', last_seq: 7 }])
  client.close()
})

test('emits events and resolves matching command acknowledgements', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws',
    token: 'control-token',
    sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory,
    reconnect: false,
  })
  const seen: AgentWharfEvent[] = []
  client.onEvent((event) => seen.push(event))

  const ackPromise = client.connect()
  sockets.last().open()
  sockets.last().receive({
    frame: 'hello.ack',
    protocol_version: 1,
    sessions: [
      { session_id: 'ses_1', state: 'ready', provider: 'claude-code', latest_seq: 0, replay_from: 1 },
    ],
  })
  await ackPromise

  const commandPromise = client.sendMessage('ses_1', [{ kind: 'text', text: 'ping' }], { commandId: 'cmd_1' })
  assert.deepEqual(sockets.last().sentFrames()[1], {
    frame: 'command',
    cmd_id: 'cmd_1',
    type: 'session.send',
    session_id: 'ses_1',
    payload: { content: [{ kind: 'text', text: 'ping' }] },
  })

  const ack: CommandAckFrame = {
    frame: 'command.ack',
    cmd_id: 'cmd_1',
    status: 'accepted',
    reason: '',
  }
  sockets.last().receive(ack)
  assert.deepEqual(await commandPromise, ack)

  sockets.last().receive({
    frame: 'event',
    type: 'session.message',
    session_id: 'ses_1',
    seq: 1,
    time: 1764937200123,
    payload: { role: 'user', content: [{ kind: 'text', text: 'ping' }] },
  })
  assert.equal(seen.length, 1)
  assert.equal(client.lastSeq('ses_1'), 1)
  client.close()
})

test('submits an opaque attach grant and keeps routing separate from execution state', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_target' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await connected

  const stateUpdates: string[] = []
  client.onDeliveryState((state) => stateUpdates.push(`${state.routing}/${state.adapter}/${state.provider}`))
  const attach = client.attach('ses_target', 'opaque.grant', { commandId: 'attach_1' })
  assert.deepEqual(sockets.last().sentFrames()[1], {
    frame: 'command', cmd_id: 'attach_1', type: 'session.attach', session_id: 'ses_target', payload: { grant: 'opaque.grant' },
  })
  sockets.last().receive({ frame: 'command.ack', cmd_id: 'attach_1', status: 'accepted', reason: '' })
  assert.equal((await attach).status, 'accepted')
  assert.deepEqual(client.deliveryState('attach_1'), {
    commandId: 'attach_1', sessionId: 'ses_target', type: 'session.attach',
    routing: 'accepted', adapter: 'pending', provider: 'pending',
  })

  sockets.last().receive({
    frame: 'event', type: 'session.attach', session_id: 'ses_target', seq: 2, time: 2,
    payload: { cmd_id: 'attach_1', delivery_state: 'received', outcome: 'completed' },
  })
  assert.equal(client.deliveryState('attach_1')?.adapter, 'received')
  assert.equal(client.deliveryState('attach_1')?.provider, 'completed')
  assert.deepEqual(stateUpdates, ['pending/pending/pending', 'accepted/pending/pending', 'accepted/received/completed'])

  sockets.last().receive({
    frame: 'event', type: 'session.attach', session_id: 'ses_other', seq: 99, time: 99,
    payload: { cmd_id: 'attach_1', delivery_state: 'outcome_unknown', outcome: 'outcome_unknown' },
  })
  assert.equal(client.deliveryState('attach_1')?.provider, 'completed')
  client.close()
})

test('resume reuses durable cursors and marks an in-flight command outcome unknown', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_1' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await connected
  sockets.last().receive({ frame: 'event', type: 'session.message', session_id: 'ses_1', seq: 7, time: 7, payload: {} })

  const command = client.sendMessage('ses_1', [{ kind: 'text', text: 'retry?' }], { commandId: 'cmd_uncertain' })
  sockets.last().serverClose()
  await assert.rejects(command, /websocket closed before command\.ack/)
  assert.equal(client.deliveryState('cmd_uncertain')?.provider, 'outcome_unknown')

  const resumed = client.resume()
  sockets.last().open()
  assert.deepEqual(sockets.last().sentFrames()[0], {
    frame: 'hello', protocol_version: 2, role: 'client', token: 'control-token',
    subscriptions: [{ session_id: 'ses_1', last_seq: 7 }],
  })
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await resumed
  client.close()
})

test('routes settings changes and keeps durable capability/effective events separate from ack', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_settings' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 1, sessions: [] })
  await connected

  const capabilities: string[] = []
  const effectives: string[] = []
  client.onSettingsCapability((update) => capabilities.push(update.capability.effectiveModelId))
  client.onSettingsEffective((update) => effectives.push(update.effective.outcome))
  const command = client.changeSettings('ses_settings', {
    commandId: 'settings_1', capabilityFingerprint: `sha256:${'a'.repeat(64)}`, modelId: 'reasoning',
  })
  assert.deepEqual(sockets.last().sentFrames()[1], {
    frame: 'command', cmd_id: 'settings_1', type: 'session.settings.change', session_id: 'ses_settings',
    payload: { capability_fingerprint: `sha256:${'a'.repeat(64)}`, model_id: 'reasoning' },
  })
  sockets.last().receive({ frame: 'command.ack', cmd_id: 'settings_1', status: 'accepted', reason: '' })
  assert.equal((await command).status, 'accepted')
  assert.equal(client.deliveryState('settings_1')?.provider, 'pending')

  const fingerprint = `sha256:${'b'.repeat(64)}`
  sockets.last().receive({
    frame: 'event', type: 'session.settings.capabilities', session_id: 'ses_settings', seq: 3, time: 3,
    payload: {
      schema_version: 1, fingerprint, models: [{ id: 'balanced', label: 'Balanced' }, { id: 'reasoning', label: 'Reasoning' }],
      permission_modes: [{ id: 'ask', label: 'Ask first' }], effective_model_id: 'reasoning', effective_permission_mode_id: 'ask',
      model_change: 'allowed', permission_change: 'read_only', model_read_only_reason: null, permission_read_only_reason: 'policy',
    },
  })
  sockets.last().receive({
    frame: 'event', type: 'session.settings.effective', session_id: 'ses_settings', seq: 4, time: 4,
    payload: {
      cmd_id: 'settings_1', request_fingerprint: `sha256:${'a'.repeat(64)}`, effective_fingerprint: fingerprint,
      outcome: 'applied', effective_model_id: 'reasoning', effective_permission_mode_id: 'ask', reason_code: null,
    },
  })
  assert.deepEqual(capabilities, ['reasoning'])
  assert.deepEqual(effectives, ['applied'])
  assert.equal(client.settingsCapability('ses_settings')?.capability.fingerprint, fingerprint)
  assert.equal(client.settingsEffective('settings_1')?.effective.outcome, 'applied')
  assert.equal(client.deliveryState('settings_1')?.provider, 'pending')
  await assert.rejects(client.changeSettings('ses_settings', { capabilityFingerprint: 'bad', modelId: 'reasoning' }), /fingerprint/)
  client.close()
})

test('keeps interrupt and stop routing acks distinct from durable run-control outcomes', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_control' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 2, sessions: [] })
  await connected

  const capabilityStates: boolean[] = []
  const outcomes: string[] = []
  client.onRunControlCapability((update) => capabilityStates.push(update.capability.stopSupported))
  client.onRunControlOutcome((update) => outcomes.push(`${update.outcome.operation}/${update.outcome.outcome}`))
  const interrupt = client.interrupt('ses_control', { commandId: 'interrupt_1' })
  sockets.last().receive({ frame: 'command.ack', cmd_id: 'interrupt_1', status: 'accepted', reason: '' })
  assert.equal((await interrupt).status, 'accepted')
  assert.equal(client.runControlOutcome('interrupt_1'), undefined)

  sockets.last().receive({
    frame: 'event', type: 'session.run.capabilities', session_id: 'ses_control', seq: 2, time: 2,
    payload: { schema_version: 1, interrupt_supported: true, stop_supported: false },
  })
  sockets.last().receive({
    frame: 'event', type: 'session.run.outcome', session_id: 'ses_control', seq: 3, time: 3,
    payload: { cmd_id: 'interrupt_1', operation: 'interrupt', outcome: 'completed', completion_state: 'ready', reason_code: null },
  })
  assert.deepEqual(capabilityStates, [false])
  assert.deepEqual(outcomes, ['interrupt/completed'])
  assert.equal(client.runControlOutcome('interrupt_1')?.outcome.completionState, 'ready')

  const stop = client.stop('ses_control', { commandId: 'stop_1' })
  sockets.last().receive({ frame: 'command.ack', cmd_id: 'stop_1', status: 'duplicate', reason: '' })
  assert.equal((await stop).status, 'duplicate')
  sockets.last().receive({
    frame: 'event', type: 'session.run.outcome', session_id: 'ses_control', seq: 4, time: 4,
    payload: { cmd_id: 'stop_1', operation: 'stop', outcome: 'unsupported', completion_state: null, reason_code: 'stop_unsupported' },
  })
  assert.equal(client.runControlOutcome('stop_1')?.outcome.outcome, 'unsupported')
  assert.equal(client.deliveryState('stop_1')?.routing, 'duplicate')
  client.close()
})

test('serializes bounded file references without content transport or path fallback', async () => {
  const sockets = new FakeSocketFactory()
  const client = new AgentWharfClient({
    url: 'ws://hub.local/ws', token: 'control-token', sessions: [{ sessionId: 'ses_files' }],
    webSocketFactory: sockets.factory, reconnect: false,
  })
  const connected = client.connect()
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 2, sessions: [] })
  await connected

  const capabilityFingerprint = `sha256:${'c'.repeat(64)}`
  const command = client.sendMessageWithReferences('ses_files', [
    { kind: 'text', text: 'Review this' },
    {
      kind: 'file_reference', disposition: 'file', path: 'src/app.ts', version: 'v1',
      contentDigest: `sha256:${'d'.repeat(64)}`, bytes: 123, mediaType: 'text/plain',
    },
  ], { capabilityFingerprint, commandId: 'file_cmd_1' })
  assert.deepEqual(sockets.last().sentFrames()[1], {
    frame: 'command', cmd_id: 'file_cmd_1', type: 'session.send', session_id: 'ses_files',
    payload: {
      content: [
        { kind: 'text', text: 'Review this' },
        { kind: 'file_reference', disposition: 'file', path: 'src/app.ts', version: 'v1', content_digest: `sha256:${'d'.repeat(64)}`, bytes: 123, media_type: 'text/plain' },
      ], capability_fingerprint: capabilityFingerprint,
    },
  })
  assert.equal(JSON.stringify(sockets.last().sentFrames()[1]).includes('base64'), false)
  sockets.last().receive({ frame: 'command.ack', cmd_id: 'file_cmd_1', status: 'accepted', reason: '' })
  assert.equal((await command).status, 'accepted')
  await assert.rejects(client.sendMessageWithReferences('ses_files', [
    { kind: 'file_reference', disposition: 'file', path: '../secret', version: 'v1', contentDigest: `sha256:${'d'.repeat(64)}`, bytes: 1 },
  ], { capabilityFingerprint }), /file-reference message part/)
  await assert.rejects(client.sendMessageWithReferences('ses_files', [
    { kind: 'file_reference', disposition: 'file', path: 'ok.txt', version: 'v1', contentDigest: `sha256:${'d'.repeat(64)}`, bytes: 10 * 1024 * 1024 + 1 },
  ], { capabilityFingerprint }), /file-reference message part/)
  client.close()
})

test('keeps attention summaries bounded, resumable and command-free', async () => {
  const sockets = new FakeSocketFactory()
  let requestNumber = 0
  const client = new AgentWharfAttentionClient({
    url: 'ws://hub.local/ws', token: 'attention-token', webSocketFactory: sockets.factory, reconnect: false,
    requestIdFactory: () => `attn_${++requestNumber}`,
  })
  const updates: string[] = []
  client.onSummary((frame) => updates.push(`${frame.kind}/${frame.subscription_state}`))
  const connected = client.connect()
  sockets.last().open()
  assert.deepEqual(sockets.last().sentFrames()[0], {
    frame: 'hello', protocol_version: 2, role: 'client', token: 'attention-token', subscriptions: [],
  })
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 2, sessions: [] })
  await Promise.resolve()
  assert.deepEqual(sockets.last().sentFrames()[1], { frame: 'attention.subscribe', request_id: 'attn_1' })
  sockets.last().receive({
    frame: 'attention.summary', request_id: 'attn_1', kind: 'snapshot', subscription_state: 'complete',
    summaries: [{
      session_id: 'ses_attention', latest_seq: 7, state: 'working', summary_version: 3, summary_state: 'complete',
      blocker: { kind: 'queued', reason: 'capacity', expires_at: 123, blocking_session_id: 'ses_attention' },
    }],
  })
  const snapshot = await connected
  assert.equal(snapshot.summaries.length, 1)
  assert.equal(client.summary('ses_attention')?.summaryVersion, 3)
  assert.equal(client.summary('ses_attention')?.blocker?.kind, 'queued')
  assert.equal('sendMessage' in client, false)

  const refreshed = client.refresh()
  assert.deepEqual(sockets.last().sentFrames()[2], { frame: 'attention.subscribe', request_id: 'attn_2' })
  sockets.last().receive({
    frame: 'attention.summary', request_id: 'attn_2', kind: 'update', subscription_state: 'incomplete',
    summaries: [{
      session_id: 'ses_attention', latest_seq: 8, state: 'working', summary_version: 4, summary_state: 'incomplete',
      blocker: { kind: 'outcome_unknown', operation: 'start' },
    }],
  })
  await refreshed
  assert.equal(client.summary('ses_attention')?.latestSeq, 8)
  assert.equal(client.currentSubscriptionState(), 'incomplete')
  sockets.last().receive({
    frame: 'attention.summary', request_id: 'live', kind: 'update', subscription_state: 'complete',
    summaries: [{ session_id: 'ses_attention', latest_seq: 7, state: 'working', summary_version: 3, summary_state: 'complete' }],
  })
  assert.equal(client.summary('ses_attention')?.summaryVersion, 4)
  assert.deepEqual(updates, ['snapshot/complete', 'update/incomplete', 'update/complete'])

  const switched = client.switchIdentity('attention-token-2')
  sockets.last().open()
  sockets.last().receive({ frame: 'hello.ack', protocol_version: 2, sessions: [] })
  await Promise.resolve()
  assert.equal(sockets.last().sentFrames()[0].token, 'attention-token-2')
  sockets.last().receive({ frame: 'attention.summary', request_id: 'attn_3', kind: 'snapshot', subscription_state: 'complete', summaries: [] })
  await switched
  assert.equal(client.summaries().length, 0)
  client.close()
})

class FakeSocketFactory {
  readonly all: FakeSocket[] = []

  readonly factory: WebSocketFactory = (url: string) => {
    const socket = new FakeSocket(url)
    this.all.push(socket)
    return socket
  }

  last(): FakeSocket {
    const socket = this.all.at(-1)
    assert.ok(socket)
    return socket
  }
}

class FakeSocket {
  onopen: (() => void) | null = null
  onmessage: ((event: MessageEvent<string>) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null

  private readonly sent: string[] = []
  private closed = false
  private deferClose = false

  constructor(readonly url: string) {}

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.closed = true
    if (!this.deferClose) {
      this.finishClose()
    }
  }

  open(): void {
    this.onopen?.()
  }

  receive(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) } as MessageEvent<string>)
  }

  serverClose(): void {
    this.onclose?.({ wasClean: false } as CloseEvent)
  }

  deferCloseEvent(): void {
    this.deferClose = true
  }

  finishClose(): void {
    this.onclose?.({ wasClean: true } as CloseEvent)
  }

  sentFrames(): any[] {
    return this.sent.map((line) => JSON.parse(line))
  }

  isClosed(): boolean {
    return this.closed
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 1000
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('timed out waiting for predicate')
    }
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
