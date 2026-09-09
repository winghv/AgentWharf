// Isolated feasibility prototype. Not exported by the SDK or negotiated with Hub.
export const MAX_CONTENT_BYTES = 32768
export interface ContentContext {
  scope: 'command' | 'event' | 'launch'
  session: string
  sender: string
  keyId: string
  messageId: string
  type: string
}
export interface ContentEnvelope {
  nonce: string
  ciphertext: string
  signature: string
}

function invalid(): never { throw new Error('invalid encrypted content') }
function contextBytes(context: ContentContext): Uint8Array<ArrayBuffer> {
  const values = [context.scope, context.session, context.sender, context.keyId, context.messageId, context.type]
  if (!['command', 'event', 'launch'].includes(context.scope) || values.some(value => typeof value !== 'string' || !/^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) invalid()
  return new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.prototype.v1', ...values]))
}
function concat(...values: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const result = new Uint8Array(values.reduce((size, value) => size + value.length, 0))
  let offset = 0
  for (const value of values) { result.set(value, offset); offset += value.length }
  return result
}
function encode(value: Uint8Array): string {
  let binary = ''
  for (const byte of value) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
function decode(value: string, min: number, max: number): Uint8Array<ArrayBuffer> {
  if (typeof value !== 'string' || value.length > Math.ceil(max * 4 / 3) || !/^[A-Za-z0-9_-]+$/.test(value)) invalid()
  const binary = atob(value.replace(/-/g, '+').replace(/_/g, '/'))
  const result = Uint8Array.from(binary, char => char.charCodeAt(0))
  if (result.length < min || result.length > max || encode(result) !== value) invalid()
  return result
}
async function contentKey(key: Uint8Array, usage: KeyUsage): Promise<CryptoKey> {
  if (key.length !== 32) invalid()
  return crypto.subtle.importKey('raw', new Uint8Array(key), 'AES-GCM', false, [usage])
}
export async function sealContent(context: ContentContext, key: Uint8Array, signingKey: CryptoKey, plaintext: Uint8Array): Promise<ContentEnvelope> {
  try {
    if (plaintext.length > MAX_CONTENT_BYTES || signingKey.algorithm.name !== 'Ed25519' || signingKey.type !== 'private') invalid()
    const aad = contextBytes(context)
    const nonce = crypto.getRandomValues(new Uint8Array(12))
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv: nonce, additionalData: aad, tagLength: 128 }, await contentKey(key, 'encrypt'), new Uint8Array(plaintext)))
    const signature = new Uint8Array(await crypto.subtle.sign('Ed25519', signingKey, concat(aad, nonce, ciphertext)))
    return { nonce: encode(nonce), ciphertext: encode(ciphertext), signature: encode(signature) }
  } catch { return invalid() }
}
export async function openContent(context: ContentContext, key: Uint8Array, verifyKey: CryptoKey, envelope: ContentEnvelope): Promise<Uint8Array<ArrayBuffer>> {
  try {
    if (verifyKey.algorithm.name !== 'Ed25519' || verifyKey.type !== 'public' || Object.keys(envelope).sort().join(',') !== 'ciphertext,nonce,signature') invalid()
    const aad = contextBytes(context)
    const nonce = decode(envelope.nonce, 12, 12)
    const ciphertext = decode(envelope.ciphertext, 16, MAX_CONTENT_BYTES + 16)
    const signature = decode(envelope.signature, 64, 64)
    if (!await crypto.subtle.verify('Ed25519', verifyKey, signature, concat(aad, nonce, ciphertext))) invalid()
    return new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: nonce, additionalData: aad, tagLength: 128 }, await contentKey(key, 'decrypt'), ciphertext))
  } catch { return invalid() }
}
