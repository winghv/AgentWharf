import assert from 'node:assert/strict'
import test from 'node:test'
import { openPacket, sealPacket } from '../src/experimental/packet.js'
import { sealContent, type ContentContext } from '../src/experimental/e2ee.js'

test('encrypted packet preserves domain payload and authenticates visible state', async () => {
  const keys = await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']) as CryptoKeyPair
  const key = crypto.getRandomValues(new Uint8Array(32))
  const context: ContentContext = { scope: 'event', session: 'session', keyId: 'key', sender: 'sender', messageId: 'event_1', type: 'session.state' }
  const payload = { state: 'busy', metadata: { title: 'synthetic <private> title', cwd: 'directory' } }
  const packet = await sealPacket(context, key, keys.privateKey, { state: 'busy' }, payload)
  assert.deepEqual(await openPacket(context, key, keys.publicKey, packet), payload)
  await assert.rejects(openPacket(context, key, keys.publicKey, { ...packet, public: { state: 'ready' } }))
  await assert.rejects(openPacket(context, key, keys.publicKey, { ...packet, public: { state: 'busy', role: 'user' } }))
  await assert.rejects(openPacket({ ...context, type: 'session.message' }, key, keys.publicKey, packet))
  await assert.rejects(sealPacket(context, key, keys.privateKey, { state: 'ready' }, payload))
  const inconsistent = await sealContent(context, key, keys.privateKey, new TextEncoder().encode(JSON.stringify({ public: { state: 'ready' }, payload })))
  await assert.rejects(openPacket(context, key, keys.publicKey, { version: 1, public: { state: 'ready' }, encrypted: inconsistent }))
  const ambiguous = await sealContent(context, key, keys.privateKey, new TextEncoder().encode('{"public":{"state":"busy"},"payload":{"state":"busy","state":"ready"}}'))
  await assert.rejects(openPacket(context, key, keys.publicKey, { version: 1, public: { state: 'busy' }, encrypted: ambiguous }))
  const wire = JSON.stringify(packet)
  assert.equal(wire.includes('synthetic'), false)
  assert.equal(wire.includes('directory'), false)
})

test('run-control public projections open with current and legacy shapes', async () => {
  const keys = await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']) as CryptoKeyPair
  const key = crypto.getRandomValues(new Uint8Array(32))
  const outcomeContext: ContentContext = { scope: 'event', session: 'session', keyId: 'key', sender: 'machine', messageId: 'event_1', type: 'session.run.outcome' }
  const outcome = { cmd_id: 'cmd_1', operation: 'stop', outcome: 'completed', completion_state: 'ended', reason_code: null }
  const outcomePacket = await sealPacket(outcomeContext, key, keys.privateKey, { operation: 'stop', outcome: 'completed', completion_state: 'ended' }, outcome)
  assert.deepEqual(await openPacket(outcomeContext, key, keys.publicKey, outcomePacket), outcome)

  const capabilityContext: ContentContext = { scope: 'event', session: 'session', keyId: 'key', sender: 'machine', messageId: 'event_2', type: 'session.run.capabilities' }
  const capability = { schema_version: 1, interrupt_supported: true, stop_supported: false }
  const capabilityPacket = await sealPacket(capabilityContext, key, keys.privateKey, { interrupt_supported: true }, capability)
  assert.deepEqual(await openPacket(capabilityContext, key, keys.publicKey, capabilityPacket), capability)

  // A legacy endpoint sealed an empty projection for the same payload.
  const legacyContent = await sealContent(outcomeContext, key, keys.privateKey, new TextEncoder().encode(JSON.stringify({ public: {}, payload: outcome })))
  const legacy = { version: 1 as const, public: {}, encrypted: legacyContent }
  assert.deepEqual(await openPacket(outcomeContext, key, keys.publicKey, legacy), outcome)
  await assert.rejects(sealPacket(outcomeContext, key, keys.privateKey, { operation: 'bogus', outcome: 'completed' }, outcome))
})
