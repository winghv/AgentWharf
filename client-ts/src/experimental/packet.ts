import { decodeStrictJson } from './strictJson.js'
import { openContent, sealContent, type ContentContext, type ContentEnvelope, type ContentKey } from './e2ee.js'

export interface PublicMetadata {
  state?: string; role?: string; request_id?: string; decision?: string
  operation?: string; outcome?: string; completion_state?: string; reason_code?: string
  interrupt_supported?: boolean; stop_supported?: boolean
}
export interface ContentPacket { version: 1; public: PublicMetadata; encrypted: ContentEnvelope }
function invalid(): never { throw new Error('invalid encrypted packet') }
const runControlTypes = new Set(['session.run.outcome', 'session.run.capabilities'])
function validatePublic(type: string, value: PublicMetadata): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid()
  const keys = Object.keys(value).sort().join(',')
  switch (type) {
    case 'session.state':
      if (keys !== 'state' || !['starting', 'ready', 'busy', 'waiting_permission', 'ended', 'error'].includes(value.state ?? '')) invalid()
      return
    case 'session.message':
      if (keys !== 'role' || !['user', 'agent', 'system'].includes(value.role ?? '')) invalid()
      return
    case 'permission.request':
      if (keys !== 'request_id' || !validId(value.request_id)) invalid()
      return
    case 'permission.decision':
    case 'permission.respond':
      if (keys !== 'decision,request_id' || !validId(value.request_id) || !['approve', 'deny', 'expired'].includes(value.decision ?? '')) invalid()
      return
    case 'session.run.outcome': {
      // A legacy endpoint sealed an empty projection; either shape is accepted.
      if (keys === '') return
      const allowed = new Set(['operation', 'outcome', 'completion_state', 'reason_code'])
      for (const key of Object.keys(value)) if (!allowed.has(key)) invalid()
      if (!['interrupt', 'stop'].includes(value.operation ?? '')) invalid()
      if (!['completed', 'rejected', 'timeout', 'outcome_unknown'].includes(value.outcome ?? '')) invalid()
      if (value.completion_state !== undefined && !['ready', 'ended'].includes(value.completion_state)) invalid()
      if (value.reason_code !== undefined && !validId(value.reason_code)) invalid()
      return
    }
    case 'session.run.capabilities': {
      const allowed = new Set(['interrupt_supported', 'stop_supported'])
      for (const key of Object.keys(value)) {
        if (!allowed.has(key) || typeof (value as Record<string, unknown>)[key] !== 'boolean') invalid()
      }
      return
    }
    default:
      if (keys !== '') invalid()
  }
}
function validId(value: unknown): boolean { return typeof value === 'string' && /^[A-Za-z0-9_.:/-]{1,128}$/.test(value) }
function normalized(value: PublicMetadata): string {
  return JSON.stringify([
    value.state ?? '', value.role ?? '', value.request_id ?? '', value.decision ?? '',
    value.operation ?? '', value.outcome ?? '', value.completion_state ?? '', value.reason_code ?? '',
    value.interrupt_supported === true, value.stop_supported === true,
  ])
}
export function projectPublicMetadata(type: string, payload: unknown): PublicMetadata {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) invalid()
  const fields = payload as Record<string, unknown>
  let projection: PublicMetadata = {}
  switch (type) {
    case 'session.state': projection = { state: fields.state as string }; break
    case 'session.message': projection = { role: fields.role as string }; break
    case 'permission.request': projection = { request_id: fields.request_id as string }; break
    case 'permission.decision':
    case 'permission.respond': projection = { request_id: fields.request_id as string, decision: fields.decision as string }; break
    case 'session.run.outcome': {
      projection = { operation: fields.operation as string, outcome: fields.outcome as string }
      if (fields.completion_state !== undefined && fields.completion_state !== null) projection.completion_state = fields.completion_state as string
      if (fields.reason_code !== undefined && fields.reason_code !== null) projection.reason_code = fields.reason_code as string
      break
    }
    case 'session.run.capabilities': {
      projectRunControlCapabilities(projection, fields)
      break
    }
  }
  validatePublic(type, projection)
  return projection
}
function projectRunControlCapabilities(projection: PublicMetadata, fields: Record<string, unknown>): void {
  if (fields.interrupt_supported === true) projection.interrupt_supported = true
  if (fields.stop_supported === true) projection.stop_supported = true
}
export async function sealPacket(context: ContentContext, key: ContentKey, signer: CryptoKey, publicMetadata: PublicMetadata, payload: unknown): Promise<ContentPacket> {
  validatePublic(context.type, publicMetadata)
  if (normalized(projectPublicMetadata(context.type, payload)) !== normalized(publicMetadata)) invalid()
  const projection = { ...publicMetadata }
  const plaintext = new TextEncoder().encode(JSON.stringify({ public: projection, payload }))
  return { version: 1, public: projection, encrypted: await sealContent(context, key, signer, plaintext) }
}
// decodeURIComponent uses strict UTF-8 and rejects malformed sequences. Hermes
// supplies TextEncoder but not TextDecoder; keep both runtime paths fail-closed.
export function decodePacketUTF8(bytes: Uint8Array): string {
  if (typeof TextDecoder !== 'undefined') return new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes)
  return decodeURIComponent(Array.from(bytes, byte => '%' + byte.toString(16).padStart(2, '0')).join(''))
}
export async function openPacket(context: ContentContext, key: ContentKey, signer: CryptoKey, packet: ContentPacket): Promise<unknown> {
  try {
    if (!packet || Object.keys(packet).sort().join(',') !== 'encrypted,public,version' || packet.version !== 1) invalid()
    validatePublic(context.type, packet.public)
    const plaintext = await openContent(context, key, signer, packet.encrypted)
    try {
      const content = decodeStrictJson(decodePacketUTF8(plaintext)) as { public: PublicMetadata; payload: unknown }
      if (!content || Object.keys(content).sort().join(',') !== 'payload,public') invalid()
      validatePublic(context.type, content.public)
      if (normalized(content.public) !== normalized(packet.public)) invalid()
      // A legacy endpoint sealed an empty projection for run-control events; its
      // payload still drives the endpoint-owned outcome, so accept the empty
      // public only for those types instead of rejecting valid history.
      const legacyRunControlProjection = runControlTypes.has(context.type) && Object.keys(packet.public).length === 0
      if (!legacyRunControlProjection && normalized(projectPublicMetadata(context.type, content.payload)) !== normalized(content.public)) invalid()
      return content.payload
    } finally { plaintext.fill(0) }
  } catch { return invalid() }
}
