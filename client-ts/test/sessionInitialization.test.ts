import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { signSessionInitialization } from '../src/experimental/sessionInitialization.js'

test('session initialization signatures interoperate with Go strict decoder', { timeout: 120000 }, async () => {
  const seed = Buffer.from(Array.from({ length: 32 }, (_, i) => i + 32))
  const signing = await crypto.subtle.importKey('pkcs8', Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed]), 'Ed25519', false, ['sign'])
  const input = { machine: 'machine', account: 'account', session: 'session', key_id: 'key', device: 'client' }
  const request = await signSessionInitialization(input, signing)
  const output = await new Promise<string>((resolve, reject) => {
    const child = execFile('go', ['run', './experimental/e2ee/testdata/sessioninit'], {
      cwd: fileURLToPath(new URL('../../..', import.meta.url)),
      env: { ...process.env, GOWORK: 'off' }, timeout: 110000, maxBuffer: 4096,
    }, (err, stdout) => err ? reject(err) : resolve(stdout))
    child.stdin!.end(JSON.stringify(request))
  })
  const result = JSON.parse(output)
  assert.equal(result.signature, request.signature)
  const publicKey = await crypto.subtle.importKey('raw', Buffer.from(result.public, 'base64url'), 'Ed25519', false, ['verify'])
  const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.session-init.v1', ...Object.values(input)]))
  assert.equal(await crypto.subtle.verify('Ed25519', publicKey, Buffer.from(result.signature, 'base64url'), message), true)
  await assert.rejects(signSessionInitialization({ ...input, session: '' }, signing))
})
