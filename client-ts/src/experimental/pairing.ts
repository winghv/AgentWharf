import { Aes256Gcm, CipherSuite, DhkemP256HkdfSha256, HkdfSha256 } from '@hpke/core'

export interface PairingOffer { version: 1; id: string; machine: string; public_key: string; secret: string; expires_at: number }
export interface PairingIdentity { device: string; signing_key: string; wrapping_key: string }
export interface PairingRequest { enc: string; ciphertext: string }
export interface PairingReceipt { confirmation: string }
export interface PendingPairing { request: PairingRequest; verifyReceipt(receipt: PairingReceipt): Promise<void> }
const suite = new CipherSuite({ kem: new DhkemP256HkdfSha256(), kdf: new HkdfSha256(), aead: new Aes256Gcm() })
function invalid(): never { throw new Error('invalid pairing request') }
function encode(data: ArrayBuffer): string { return btoa(String.fromCharCode(...new Uint8Array(data))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '') }
function decode(value: string, size: number): Uint8Array<ArrayBuffer> {
  if (typeof value !== 'string' || value.length !== Math.ceil(size * 4 / 3) || !/^[A-Za-z0-9_-]+$/.test(value)) invalid()
  const data = Uint8Array.from(atob(value.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0))
  if (data.length !== size || encode(data.buffer) !== value) invalid()
  return data
}
// Offer comes only from the existing local scan/paste action, never a server
// response. No caller may upload or log offer.secret or the complete offer.
export async function encryptPairingIdentity(offer: PairingOffer, identity: PairingIdentity, signingKey: CryptoKey, now = Date.now()): Promise<PairingRequest> {
  return (await preparePairing(offer, identity, signingKey, now)).request
}
export async function preparePairing(offer: PairingOffer, identity: PairingIdentity, signingKey: CryptoKey, now = Date.now()): Promise<PendingPairing> {
  try {
    if (offer.version !== 1 || !Number.isSafeInteger(offer.expires_at) || offer.expires_at <= now || offer.expires_at > now + 300000) invalid()
    if (![offer.machine, identity.device].every(value => typeof value === 'string' && /^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) invalid()
    decode(offer.id, 16)
    const secret = decode(offer.secret, 32)
    const recipientPublicKey = await suite.kem.deserializePublicKey(decode(offer.public_key, 65))
    decode(identity.signing_key, 32)
    await suite.kem.deserializePublicKey(decode(identity.wrapping_key, 65))
    const aad = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.pair.v1', offer.id, offer.machine, offer.public_key, offer.expires_at]))
    const sender = await suite.createSenderContext({ recipientPublicKey, psk: { key: secret, id: new TextEncoder().encode(offer.id) }, info: await crypto.subtle.digest('SHA-256', aad) })
    const plaintext = new TextEncoder().encode(JSON.stringify({ device: identity.device, signing_key: identity.signing_key, wrapping_key: identity.wrapping_key }))
    const domain = new TextEncoder().encode('agentwharf.e2ee.pair.proof.v1\0')
    const proofInput = new Uint8Array(domain.length + aad.length + 1 + plaintext.length)
    proofInput.set(domain)
    proofInput.set(aad, domain.length)
    proofInput.set(plaintext, domain.length + aad.length + 1)
    const proof = await crypto.subtle.sign('Ed25519', signingKey, proofInput)
    const publicKey = await crypto.subtle.importKey('raw', decode(identity.signing_key, 32), 'Ed25519', false, ['verify'])
    if (!await crypto.subtle.verify('Ed25519', publicKey, proof, proofInput)) invalid()
    const protectedIdentity = new TextEncoder().encode(JSON.stringify({ identity: { device: identity.device, signing_key: identity.signing_key, wrapping_key: identity.wrapping_key }, proof: encode(proof) }))
    const request = { enc: encode(sender.enc), ciphertext: encode(await sender.seal(protectedIdentity, aad)) }
    const confirmationBytes = await sender.export(new TextEncoder().encode('agentwharf.e2ee.pair.confirm.v1'), 32)
    const confirmationKey = await crypto.subtle.importKey('raw', confirmationBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
    new Uint8Array(confirmationBytes).fill(0)
    return {
      request,
      async verifyReceipt(receipt: PairingReceipt): Promise<void> {
        try {
          if (Object.keys(receipt).join(',') !== 'confirmation' || !await crypto.subtle.verify('HMAC', confirmationKey, decode(receipt.confirmation, 32), plaintext)) invalid()
        } catch { invalid() }
      },
    }
  } catch { return invalid() }
}
