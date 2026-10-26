import type { ConnectionId, FipsTcpEndpoint } from '@fips/tcp'
import type { DirectFileSource, DirectFileTcpOptions, DirectFileClaim, DirectFileEvent } from './types.js'
import { DirectFileReader, type Pending } from './wire.js'
import type { Work } from './work.js'
import type { ReceiveFiles, SendFiles } from './files.js'
export const OFFER_MS = 24 * 60 * 60 * 1000
export interface Offer {
  token: string; peers: Set<string>; files: DirectFileSource[]; expires: number; version: 1 | 2
}
export type Role = SendFiles | ReceiveFiles | { kind: 'request' | 'finish' | 'decline' | 'announce' | 'verifying' }
export interface Connection {
  transferId: string; token: string; manifest: string; peer: string; version: 1 | 2
  role: Role; reader: DirectFileReader; pending?: Pending; doneSent: boolean
  cancelled: boolean; revoked: boolean; lastActivity: number; lastProgress: number
  terminal: boolean; ready: boolean; reserved: number; claim?: DirectFileClaim
  side: 'send' | 'receive'; completion?: DirectFileEvent
  work?: Work; waitUntil?: number; expires: number
}
export interface Runtime {
  tcp: FipsTcpEndpoint; localPeer: string; options: DirectFileTcpOptions
  offers: Map<string, Offer>; connections: Map<ConnectionId, Connection>
  ended: Set<string>; memoryReserved: number; stopped: boolean
  now(): number
}
export function newConnection(r: Runtime, transferId: string, peer: string, role: Role): Connection {
  return { transferId, token: '', manifest: '', peer, version: 1, role,
    reader: new DirectFileReader(), doneSent: false, cancelled: false, revoked: false,
    expires: r.now() + OFFER_MS, terminal: false, ready: true, reserved: 0, side: 'send', lastActivity: r.now(), lastProgress: r.now() }
}
export function emit(r: Runtime, event: Parameters<DirectFileTcpOptions['onEvent']>[0]): void {
  try { r.options.onEvent(event) } catch (error) {
    r.options.onError?.(error instanceof Error ? error : new Error(String(error)))
  }
}
export function rememberEnded(r: Runtime, id: string): void {
  if (!id) return
  r.ended.add(id)
  if (r.ended.size > 256) r.ended.delete(r.ended.values().next().value!)
}
export async function saveClaim(r: Runtime, side: 'send' | 'receive', id: string, claim: DirectFileClaim): Promise<void> {
  if (!r.options.store) throw new Error('Persistent file transfer storage is unavailable')
  await r.options.store.save(`${side}/${id}`, claim)
}
export async function cleanup(r: Runtime, c: Connection, preserve = false): Promise<void> {
  c.work?.controller.abort(new Error('File work stopped')); c.work = undefined
  r.memoryReserved -= c.reserved; c.reserved = 0
  if (c.role.kind === 'receiving') await c.role.cleanup(preserve)
}
export async function terminalClaim(r: Runtime, c: Connection, status: DirectFileClaim['status']): Promise<void> {
  if (!c.claim) return
  c.claim = { ...c.claim, status }
  await saveClaim(r, c.side, c.transferId, c.claim)
}
