import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { sealPacket } from '../src/experimental/packet.js'

test('TS command carrier passes Go durable admission and restart deduplication', { timeout: 120000 }, async () => {
  const child = spawn('go', ['run', './experimental/e2ee/testdata/commandwire'], {
    cwd: fileURLToPath(new URL('../../..', import.meta.url)),
    env: { ...process.env, GOWORK: 'off' },
    stdio: ['pipe', 'pipe', 'pipe'],
    timeout: 110000,
  })
  let stderr = ''
  child.stderr.on('data', data => { stderr = (stderr + String(data)).slice(-4096) })
  const exited = new Promise<number | null>((resolve, reject) => {
    child.once('error', reject)
    child.once('close', resolve)
  })
  // Attach rejection handling immediately, even while awaiting the first line.
  void exited.catch(() => {})
  const lines = createInterface({ input: child.stdout })
  const iterator = lines[Symbol.asyncIterator]()
  try {
    const first = await iterator.next()
    assert.equal(first.done, false, 'endpoint did not initialize')
    const fixture = JSON.parse(first.value!) as { key: string }
    const key = new Uint8Array(Buffer.from(fixture.key, 'base64url'))
    const seed = Buffer.from(Array.from({ length: 32 }, (_, i) => i + 32))
    const privateKey = await crypto.subtle.importKey('pkcs8', Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed]), 'Ed25519', false, ['sign'])
    const packet = await sealPacket({ scope: 'command', session: 'session', sender: 'client', keyId: 'key', messageId: 'command', type: 'session.send' }, key, privateKey, {}, { content: [{ kind: 'text', text: 'synthetic wire instruction' }] })
    key.fill(0)
    const wire = { version: 1, scope: 'command', key_id: 'key', sender: 'client', message_id: 'command', type: 'session.send', packet }
    assert.equal(JSON.stringify(wire).includes('synthetic wire instruction'), false)
    child.stdin.end(JSON.stringify({ Wire: wire }))
    const result = await iterator.next()
    assert.equal(result.done, false, stderr)
    assert.deepEqual(JSON.parse(result.value!), { delivered: 1, restart_duplicate: true, routing_substitution_rejected: true })
    assert.equal(await exited, 0, stderr)
  } finally {
    lines.close()
    if (child.exitCode === null) child.kill()
    await exited.catch(() => {})
  }
})
