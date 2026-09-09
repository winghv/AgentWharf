import { Aes256Gcm, CipherSuite, DhkemP256HkdfSha256, HkdfSha256 } from '@hpke/core'

export interface WrapContext { session: string; keyId: string; sender: string; recipient: string }
export interface WrappedKey { enc: string; ciphertext: string }
const suite = new CipherSuite({ kem: new DhkemP256HkdfSha256(), kdf: new HkdfSha256(), aead: new Aes256Gcm() })
function invalid(): never { throw new Error('invalid wrapped key') }
function contextBytes(context: WrapContext): Uint8Array<ArrayBuffer> {
  const values = [context.session, context.keyId, context.sender, context.recipient]
  if (values.some(value => typeof value !== 'string' || !/^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) invalid()
  return new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.keywrap.v1', ...values]))
}
function encode(bytes: ArrayBuffer): string {
  return btoa(String.fromCharCode(...new Uint8Array(bytes))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
function decode(value: string, size: number): Uint8Array<ArrayBuffer> {
  if (typeof value !== 'string' || value.length !== Math.ceil(size * 4 / 3) || !/^[A-Za-z0-9_-]+$/.test(value)) invalid()
  const bytes = Uint8Array.from(atob(value.replace(/-/g, '+').replace(/_/g, '/')), char => char.charCodeAt(0))
  if (bytes.length !== size || encode(bytes.buffer) !== value) invalid()
  return bytes
}
export async function generateWrappingKey(): Promise<CryptoKeyPair> { return suite.kem.generateKeyPair() }
export async function wrappingPublicKey(key: CryptoKey): Promise<ArrayBuffer> { return suite.kem.serializePublicKey(key) }
export async function importWrappingPublicKey(key: ArrayBuffer): Promise<CryptoKey> { return suite.kem.deserializePublicKey(key) }
export async function importWrappingPrivateKey(key: ArrayBuffer): Promise<CryptoKey> { return suite.kem.deserializePrivateKey(key) }
export async function wrapKey(context: WrapContext, senderPrivate: CryptoKey | CryptoKeyPair, recipientPublic: CryptoKey, key: Uint8Array): Promise<WrappedKey> {
  try {
    if (key.length !== 32) invalid()
    const aad = contextBytes(context)
    const sender = await suite.createSenderContext({ recipientPublicKey: recipientPublic, senderKey: senderPrivate, info: await crypto.subtle.digest('SHA-256', aad) })
    return { enc: encode(sender.enc), ciphertext: encode(await sender.seal(key, aad)) }
  } catch { return invalid() }
}
export async function unwrapKey(context: WrapContext, recipientPrivate: CryptoKey | CryptoKeyPair, senderPublic: CryptoKey, wrapped: WrappedKey): Promise<Uint8Array> {
  try {
    if (Object.keys(wrapped).sort().join(',') !== 'ciphertext,enc') invalid()
    const aad = contextBytes(context)
    const enc = decode(wrapped.enc, 65)
    const ciphertext = decode(wrapped.ciphertext, 48)
    const recipient = await suite.createRecipientContext({ recipientKey: recipientPrivate, senderPublicKey: senderPublic, enc, info: await crypto.subtle.digest('SHA-256', aad) })
    const key = new Uint8Array(await recipient.open(ciphertext, aad))
    if (key.length !== 32) invalid()
    return key
  } catch { return invalid() }
}
