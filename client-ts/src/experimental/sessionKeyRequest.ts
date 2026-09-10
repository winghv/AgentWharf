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
    return Object.freeze({...request, signature: base64url(signature)})
  } finally {message.fill(0)}
}

// TrustedSessionKeyRequest is the v2 account-terminal-trust carrier. It
// publishes the recipient public keys so a machine with trust enabled can
// enroll this terminal without a locally transferred offer.
export type TrustedSessionKeyRequest = {
  machine: string
  account: string
  session: string
  key_id: string
  device: string
  signing_key: string
  wrapping_key: string
  signature?: string
}

export async function signTrustedSessionKeyRequest(input: Omit<TrustedSessionKeyRequest, 'signature'>, signingKey: CryptoKey): Promise<Readonly<TrustedSessionKeyRequest>> {
  const request = {...input}
  if (Object.keys(request).sort().join(',') !== 'account,device,key_id,machine,session,signing_key,wrapping_key') throw new Error('invalid trusted session key request')
  const values = [request.machine, request.account, request.session, request.key_id, request.device, request.signing_key, request.wrapping_key]
  if (!values.every(value => typeof value === 'string' && /^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) throw new Error('invalid trusted session key request')
  const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.key-request.v2', ...values]))
  try {
    const signature = new Uint8Array(await crypto.subtle.sign('Ed25519', signingKey, message))
    return Object.freeze({...request, signature: base64url(signature)})
  } finally {message.fill(0)}
}

function base64url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
