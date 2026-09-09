import assert from 'node:assert/strict'
import {execFile} from 'node:child_process'
import {fileURLToPath} from 'node:url'
import test from 'node:test'
import {signSessionMembershipChange} from '../src/experimental/membership.js'

test('membership signatures interoperate with endpoint decoder and reject ambiguous membership', {timeout: 120000}, async () => {
  const seed = Buffer.from(Array.from({length: 32}, (_, i) => i + 32))
  const key = await crypto.subtle.importKey('pkcs8', Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed]), 'Ed25519', false, ['sign'])
  const input = {machine: 'machine', account: 'account', session: 'session', device: 'controller', key_id: 'next-key', expected_epoch: 1, members: [{device: 'controller', control: true}, {device: 'viewer', control: false}]}
  const signed = await signSessionMembershipChange(input, key)
  const output = await new Promise<string>((resolve, reject) => {
    const child = execFile('go', ['run', './experimental/e2ee/testdata/membership'], {cwd: fileURLToPath(new URL('../../..', import.meta.url)), env: {...process.env, GOWORK: 'off'}, timeout: 110000, maxBuffer: 4096}, (err, stdout) => err ? reject(err) : resolve(stdout))
    child.stdin!.end(JSON.stringify(signed))
  })
  assert.deepEqual(JSON.parse(output), {verified: true})
  await assert.rejects(signSessionMembershipChange({...input, members: [input.members[0]!, input.members[0]!]}, key))
  await assert.rejects(signSessionMembershipChange({...input, expected_epoch: Number.MAX_SAFE_INTEGER}, key))
})
