import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { preparePairing, type PendingPairing, type PairingOffer } from '../src/experimental/pairing.js'
import { generateWrappingKey, wrappingPublicKey } from '../src/experimental/keywrap.js'

async function exchange(change?: (offer: PairingOffer) => PairingOffer): Promise<void> {
  const pair = await generateWrappingKey()
  const signing = await crypto.subtle.generateKey('Ed25519', true, ['sign', 'verify']) as CryptoKeyPair
  const identity = { device: 'device_fixture', signing_key: Buffer.from(await crypto.subtle.exportKey('raw', signing.publicKey)).toString('base64url'), wrapping_key: Buffer.from(await wrappingPublicKey(pair.publicKey)).toString('base64url') }
  const child = spawn('go', ['run', './experimental/e2ee/testdata/pairing'], { cwd: fileURLToPath(new URL('../../..', import.meta.url)), env: { ...process.env, GOWORK: 'off' }, stdio: ['pipe', 'pipe', 'pipe'] })
  const lines = createInterface({ input: child.stdout })
  const output: string[] = []
  let error: unknown
  let pending: PendingPairing | undefined
  const timeout = setTimeout(() => child.kill('SIGKILL'), 120000)
  const done = new Promise<number | null>((resolve, reject) => { child.once('error', reject); child.once('close', resolve) })
  // Keep bounded diagnostic output, never include the offer or keys in assertions.
  let diagnostics = ''
  child.stderr.on('data', chunk => { diagnostics = (diagnostics + String(chunk)).slice(-4096) })
  lines.on('line', line => {
    output.push(line)
    if (output.length !== 1) return
    void (async () => {
      const offer = JSON.parse(line) as PairingOffer
      pending = await preparePairing(change ? change(offer) : offer, identity, signing.privateKey)
      child.stdin.end(JSON.stringify(pending.request) + '\n')
    })().catch(cause => { error = cause; child.kill() })
  })
  try {
    const code = await done
    if (error) throw error
    if (change) {
      assert.equal(code, 1, 'expected explicit cryptographic rejection, not a timeout or signal')
      assert.equal(output.length, 1, 'rejected pairing must not return an enrolled identity')
      assert.match(diagnostics, /^pairing interop rejected\r?\nexit status 1\r?\n$/)
      return
    }
    assert.equal(code, 0, 'Go rejected TS pairing request')
    assert.equal(output.length, 2)
    const response = JSON.parse(output[1])
    assert.deepEqual(response.identity, identity)
    assert.ok(pending)
    await pending.verifyReceipt(response.receipt)
    await assert.rejects(pending.verifyReceipt({ confirmation: Buffer.alloc(32).toString('base64url') }))
    await assert.rejects(pending.verifyReceipt({ confirmation: response.receipt.confirmation + '=' }))
    const different = await preparePairing(JSON.parse(output[0]), identity, signing.privateKey)
    await assert.rejects(different.verifyReceipt(response.receipt))
  } finally { clearTimeout(timeout); lines.close(); if (child.exitCode === null) child.kill() }
}

test('Go invitation accepts TS HPKE PSK identity once', { timeout: 125000 }, async () => { await exchange() })
test('relay lacking local secret cannot enroll a device', { timeout: 125000 }, async () => { await exchange(offer => ({ ...offer, secret: Buffer.alloc(32).toString('base64url') })) })
test('pairing ciphertext is bound to the original machine', { timeout: 125000 }, async () => { await exchange(offer => ({ ...offer, machine: 'other_machine' })) })
