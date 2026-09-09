export interface SessionInitialization {
  machine: string
  account: string
  session: string
  key_id: string
  device: string
  signature: string
}

// The caller supplies a locally bound account and a paired machine, not relay
// assertions. Signing authorizes epoch one only, never a membership update.
export async function signSessionInitialization(
  input: Omit<SessionInitialization, 'signature'>,
  signingKey: CryptoKey,
): Promise<Readonly<SessionInitialization>> {
  const request = { ...input }
  if (Object.keys(request).sort().join(',') !== 'account,device,key_id,machine,session') {
    throw new Error('invalid session initialization')
  }
  const values = [request.machine, request.account, request.session, request.key_id, request.device]
  if (values.some(value => typeof value !== 'string' || !/^[A-Za-z0-9_.:/-]{1,128}$/.test(value))) {
    throw new Error('invalid session initialization')
  }
  const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.session-init.v1', ...values]))
  try {
    const signature = new Uint8Array(await crypto.subtle.sign('Ed25519', signingKey, message))
    return Object.freeze({
      ...request,
      signature: btoa(String.fromCharCode(...signature)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''),
    })
  } finally {
    message.fill(0)
  }
}
