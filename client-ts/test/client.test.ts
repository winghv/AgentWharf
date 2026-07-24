import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  AgentWharfClient,
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
    protocol_version: 1,
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
    protocol_version: 1,
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
