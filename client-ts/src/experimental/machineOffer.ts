import { preparePairing, type PairingOffer, type PairingIdentity, type PendingPairing, type PairingReceipt } from './pairing.js'

export interface MachineOffer { offer: PairingOffer; identity: PairingIdentity; proof: string }
export interface PendingMachinePairing extends PendingPairing {
  machineIdentity: Readonly<PairingIdentity>
  commit(receipt: PairingReceipt, persist: (machine: string, identity: Readonly<PairingIdentity>) => Promise<void>): Promise<void>
}
function invalid(): never { throw new Error('invalid local machine offer') }
function decode(value: string, size: number): Uint8Array<ArrayBuffer> {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]+$/.test(value) || value.length !== Math.ceil(size * 4 / 3)) invalid()
  const bytes = Uint8Array.from(atob(value.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0))
  const canonical = btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  if (bytes.length !== size || canonical !== value) invalid()
  return bytes
}

// Input must come from the local scan/paste path. A self-signed offer fetched
// from the platform does not establish independent endpoint trust.
export async function prepareMachinePairing(local: MachineOffer, identity: PairingIdentity, signingKey: CryptoKey, now = Date.now()): Promise<PendingMachinePairing> {
  let stage = 'shape'
  try {
    if (!local || Object.keys(local).sort().join(',') !== 'identity,offer,proof') invalid()
    const offer = { ...local.offer }
    const machine = Object.freeze({ ...local.identity })
    const proof = local.proof
    if (!offer || Object.keys(offer).sort().join(',') !== 'expires_at,id,machine,public_key,secret,version') invalid()
    if (!machine || Object.keys(machine).sort().join(',') !== 'device,signing_key,wrapping_key' || !/^[A-Za-z0-9_.:/-]{1,128}$/.test(machine.device)) invalid()
    stage = 'signing key'
    const verifyKey = await crypto.subtle.importKey('raw', decode(machine.signing_key, 32), 'Ed25519', false, ['verify'])
    stage = 'wrapping key'
    const wrappingBytes = decode(machine.wrapping_key, 65)
    stage = 'wrapping import'
    await crypto.subtle.importKey('raw', wrappingBytes, { name: 'ECDH', namedCurve: 'P-256' }, false, [])
    stage = 'proof'
    const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.machine-offer.v1', offer.version, offer.id, offer.machine, offer.public_key, offer.secret, offer.expires_at, machine.device, machine.signing_key, machine.wrapping_key]))
    try {
      if (!await crypto.subtle.verify('Ed25519', verifyKey, decode(proof, 64), message)) invalid()
    } finally { message.fill(0) }
    stage = 'client proof'
    const pending = await preparePairing(offer, identity, signingKey, now)
    return {
      ...pending,
      machineIdentity: machine,
      async commit(receipt, persist) {
        await pending.verifyReceipt(receipt)
        await persist(offer.machine, machine)
      },
    }
  } catch { throw new Error(`invalid local machine offer: ${stage}`) }
}
