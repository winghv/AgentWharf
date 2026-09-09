import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { generateWrappingKey, importWrappingPrivateKey, importWrappingPublicKey, unwrapKey, wrapKey, type WrappedKey } from '../src/experimental/keywrap.js'

const context = { session: 'session', keyId: 'key_1', sender: 'sender', recipient: 'recipient' }
function go(operation: string, wrapped?: WrappedKey): unknown {
  const result = spawnSync('go', ['run', './experimental/e2ee/testdata/keywrap'], { cwd: fileURLToPath(new URL('../../..', import.meta.url)), env: { ...process.env, GOWORK: 'off' }, input: JSON.stringify({ Operation: operation, Wrapped: wrapped }), encoding: 'utf8', timeout: 120000, maxBuffer: 8192 })
  assert.equal(result.status, 0, result.stderr || result.error?.message)
  return JSON.parse(result.stdout)
}
function raw(value: string): ArrayBuffer { return Uint8Array.from(Buffer.from(value, 'base64')).buffer }

test('HPKE Auth key wrapping interoperates with Go in both directions', async () => {
  const fixture = go('seal') as { Wrapped: WrappedKey; SenderPublic: string; RecipientPublic: string }
  const senderPublic = await importWrappingPublicKey(raw(fixture.SenderPublic))
  const recipientPublic = await importWrappingPublicKey(raw(fixture.RecipientPublic))
  const sender = new Uint8Array(32); sender[31] = 1
  const recipient = new Uint8Array(32); recipient[31] = 2
  const senderPrivate = await importWrappingPrivateKey(sender.buffer)
  const recipientPrivate = await importWrappingPrivateKey(recipient.buffer)
  const key = new Uint8Array(32).fill(42)
  assert.deepEqual(await unwrapKey(context, recipientPrivate, senderPublic, fixture.Wrapped), key)
  assert.equal(go('open', await wrapKey(context, senderPrivate, recipientPublic, key)), true)
  for (const field of Object.keys(context)) {
    await assert.rejects(unwrapKey({ ...context, [field]: 'other' }, recipientPrivate, senderPublic, fixture.Wrapped))
  }
  const attacker = await generateWrappingKey()
  await assert.rejects(unwrapKey(context, recipientPrivate, attacker.publicKey, fixture.Wrapped))
  await assert.rejects(unwrapKey(context, attacker.privateKey, senderPublic, fixture.Wrapped))
  await assert.rejects(unwrapKey(context, recipientPrivate, senderPublic, { ...fixture.Wrapped, enc: fixture.Wrapped.enc + '=' }))
  await assert.rejects(wrapKey(context, senderPrivate, recipientPublic, key.slice(1)))
})


test('HPKE Auth accepts non-extractable private handles with explicit public keys', async () => {
  async function protectedPair(): Promise<CryptoKeyPair> {
    const pair = await generateWrappingKey()
    const encoded = await crypto.subtle.exportKey('pkcs8', pair.privateKey)
    const privateKey = await crypto.subtle.importKey('pkcs8', encoded, { name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits'])
    new Uint8Array(encoded).fill(0)
    await assert.rejects(crypto.subtle.exportKey('pkcs8', privateKey))
    return { publicKey: pair.publicKey, privateKey }
  }
  const sender = await protectedPair()
  const recipient = await protectedPair()
  const key = new Uint8Array(32).fill(42)
  const wrapped = await wrapKey(context, sender, recipient.publicKey, key)
  assert.deepEqual(await unwrapKey(context, recipient, sender.publicKey, wrapped), key)
  const attacker = await protectedPair()
  await assert.rejects(unwrapKey(context, attacker, sender.publicKey, wrapped))
})
