import type {SessionInitialization} from './sessionInitialization.js'

export type SessionKeyRequest = SessionInitialization

// This signature requests only an existing key for the signing recipient. It
// never initializes a session or supplies a grant, unlike session-init.v1.
export async function signSessionKeyRequest(input: Omit<SessionKeyRequest, 'signature'>, signingKey: CryptoKey): Promise<Readonly<SessionKeyRequest>> {
  const request = {...input}
  if (Object.keys(request).sort().join(',') !== 'account,device,key_id,machine,session') throw new Error('invalid session key request')
  const values = [request.machine, request.account, request.session, request.key_id, request.device]
  if (!values.every(value => typeof value === 'string' && /^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) throw new Error('invalid session key request')
  const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.key-request.v1', ...values]))
  try {
    const signature = new Uint8Array(await crypto.subtle.sign('Ed25519', signingKey, message))
    return Object.freeze({...request, signature: btoa(String.fromCharCode(...signature)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')})
  } finally {message.fill(0)}
}
