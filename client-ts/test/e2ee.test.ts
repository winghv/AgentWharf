import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { MAX_CONTENT_BYTES, openContent, sealContent, type ContentContext, type ContentEnvelope } from '../src/experimental/e2ee.js'

const context: ContentContext = { scope: 'command', session: 'ses_test', sender: 'device_test', keyId: 'epoch_1', messageId: 'cmd_test', type: 'session.send' }
const key = Uint8Array.from({ length: 32 }, (_, i) => i)
const encoder = new TextEncoder()
const decoder = new TextDecoder()

function go(operation: string, envelope?: ContentEnvelope, plaintext?: string): unknown {
  const result = spawnSync('go', ['run', './experimental/e2ee/testdata/interop'], {
    cwd: fileURLToPath(new URL('../../..', import.meta.url)),
    env: { ...process.env, GOWORK: 'off' },
    input: JSON.stringify({ Operation: operation, Context: { Scope: context.scope, Session: context.session, Sender: context.sender, KeyID: context.keyId, MessageID: context.messageId, Type: context.type }, Envelope: envelope, Plaintext: plaintext }),
    encoding: 'utf8', timeout: 120000, maxBuffer: 256 * 1024,
  })
  assert.equal(result.status, 0, result.stderr || result.error?.message)
  return JSON.parse(result.stdout)
}

test('Go and Web Crypto decrypt and verify each other', async () => {
  const text = 'synthetic instruction \u4f60\u597d'
  const sealed = go('seal', undefined, text) as { Envelope: ContentEnvelope; PublicKey: string }
  const publicKey = await crypto.subtle.importKey('raw', Buffer.from(sealed.PublicKey, 'base64url'), 'Ed25519', false, ['verify'])
  assert.equal(decoder.decode(await openContent(context, key, publicKey, sealed.Envelope)), text)
  const prefix = Buffer.from('302e020100300506032b657004220420', 'hex')
  const seed = Buffer.from(Array.from({ length: 32 }, (_, i) => i + 32))
  const privateKey = await crypto.subtle.importKey('pkcs8', Buffer.concat([prefix, seed]), 'Ed25519', false, ['sign'])
  const envelope = await sealContent(context, key, privateKey, encoder.encode(text))
  assert.equal(go('open', envelope), text)
})

test('content rejects changed context, signer, key, tampering and invalid envelopes', async () => {
  const pair = await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']) as CryptoKeyPair
  const envelope = await sealContent(context, key, pair.privateKey, encoder.encode('synthetic secret'))
  for (const field of Object.keys(context) as (keyof ContentContext)[]) {
    const changed = { ...context, [field]: field === 'scope' ? 'event' : 'other' } as ContentContext
    await assert.rejects(openContent(changed, key, pair.publicKey, envelope), /invalid encrypted content/)
  }
  const other = await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']) as CryptoKeyPair
  await assert.rejects(openContent(context, key, other.publicKey, envelope))
  await assert.rejects(openContent(context, new Uint8Array(32), pair.publicKey, envelope))
  for (const changed of [
    { ...envelope, nonce: envelope.nonce + '=' },
    { ...envelope, ciphertext: envelope.ciphertext.slice(1) },
    { ...envelope, signature: '' },
    { ...envelope, plaintext: 'unexpected' },
  ]) await assert.rejects(openContent(context, key, pair.publicKey, changed), /invalid encrypted content/)
  await assert.rejects(sealContent(context, key, pair.privateKey, new Uint8Array(MAX_CONTENT_BYTES + 1)))
  for (const size of [0, MAX_CONTENT_BYTES]) {
    const plaintext = new Uint8Array(size)
    const sealed = await sealContent(context, key, pair.privateKey, plaintext)
    assert.deepEqual(await openContent(context, key, pair.publicKey, sealed), plaintext)
  }
})
