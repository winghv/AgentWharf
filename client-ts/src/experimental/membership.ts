export interface SessionMembershipChange {
  machine: string
  account: string
  session: string
  device: string
  key_id: string
  expected_epoch: number
  members: ReadonlyArray<Readonly<{device: string; control: boolean}>>
  signature: string
}

// Member keys are resolved by the machine's independent paired registry. The
// signer must currently control this exact session; a platform token is irrelevant.
export async function signSessionMembershipChange(input: Omit<SessionMembershipChange, 'signature'>, signingKey: CryptoKey): Promise<Readonly<SessionMembershipChange>> {
  const request = {...input, members: input.members.map(member => ({...member}))}
  if (Object.keys(request).sort().join(',') !== 'account,device,expected_epoch,key_id,machine,members,session' ||
      ![request.machine, request.account, request.session, request.device, request.key_id].every(value => typeof value === 'string' && /^[A-Za-z0-9_.:/-]{1,128}$/.test(value)) ||
      !Number.isSafeInteger(request.expected_epoch) || request.expected_epoch < 1 || request.expected_epoch >= Number.MAX_SAFE_INTEGER ||
      request.members.length < 1 || request.members.length > 32) throw new Error('invalid membership change')
  for (let i = 0; i < request.members.length; i++) {
    const member = request.members[i]!
    if (Object.keys(member).sort().join(',') !== 'control,device' || typeof member.control !== 'boolean' || typeof member.device !== 'string' || !/^[A-Za-z0-9_.:/-]{1,128}$/.test(member.device) || (i > 0 && request.members[i - 1]!.device >= member.device)) throw new Error('invalid membership change')
  }
  const message = new TextEncoder().encode(JSON.stringify(['agentwharf.e2ee.membership.v1', request.machine, request.account, request.session, request.device, request.key_id, request.expected_epoch, request.members.map(member => [member.device, member.control])]))
  try {
    const signature = new Uint8Array(await crypto.subtle.sign('Ed25519', signingKey, message))
    return Object.freeze({...request, members: Object.freeze(request.members.map(member => Object.freeze(member))), signature: btoa(String.fromCharCode(...signature)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')})
  } finally {message.fill(0)}
}
