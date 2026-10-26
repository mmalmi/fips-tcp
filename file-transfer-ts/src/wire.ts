// Extracted from Iris Chat (MIT), Copyright Iris Chat contributors.
import type { SendMarker } from '@fips/tcp'
import { abortable } from './work.js'
import { sha256 } from '@noble/hashes/sha2.js'
import type { DirectFileSource, DirectFileSpec } from './types.js'
export const DIRECT_FILE_CHUNK_BYTES = 32 * 1024
export const MAX_CONTROL_BYTES = 16 * 1024
const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })
export type Control = { type: 'accept' | 'decline'; id: string; token: string } |
  { type: 'done' | 'received' | 'cancel' } |
  { type: 'offer'; id: string; token: string; version: 2; files: { filename: string; size_bytes: number; sha256: string }[] } |
  { type: 'offered'; id: string } |
  { type: 'resume'; id: string; token: string; manifest: string; offset: number; prefix_sha256: string } |
  { type: 'ready'; id: string; offset: number } |
  { type: 'rejected'; id: string; reason: 'unavailable' | 'mismatch' | 'expired' }
export interface Pending { bytes: Uint8Array; offset: number; marker?: SendMarker }
export function validateClaim(id: string, token: string): void {
  if (!/^[a-z\d_-]{1,128}$/i.test(id) || !/^[a-f\d]{64}$/i.test(token)) throw new Error('Invalid file offer')
}
export function validateFiles(files: DirectFileSpec[]): void {
  if (!files.length || files.length > 32) throw new Error('Choose between 1 and 32 files')
  for (const file of files) {
    if (!file.filename || encoder.encode(file.filename).length > 240 || file.filename === '.' || file.filename === '..' ||
      /[\p{Cc}/\\<>:"|?*]/u.test(file.filename) || !Number.isSafeInteger(file.sizeBytes) ||
      file.sizeBytes < 0 || file.sizeBytes > 100 * 1024 * 1024 * 1024 || !/^[a-f\d]{64}$/i.test(file.sha256)) {
      throw new Error('Invalid file details')
    }
  }
}
export function normalizePeer(peer: string): string {
  const normalized = /^(02|03)[a-f\d]{64}$/i.test(peer) ? peer.slice(2) : peer
  if (!/^[a-f\d]{64}$/i.test(normalized)) throw new Error('Invalid receiving device')
  return normalized.toLowerCase()
}
export function randomSeed(): bigint {
  const bytes = crypto.getRandomValues(new Uint32Array(2))
  return (BigInt(bytes[0]) << 32n) | BigInt(bytes[1])
}
export function toHex(data: Uint8Array): string { return Array.from(data, byte => byte.toString(16).padStart(2, '0')).join('') }
export function errorText(error: unknown): string { return error instanceof Error ? error.message : String(error) }
export function pendingBytes(bytes: Uint8Array): Pending { return { bytes, offset: 0 } }
export function pendingControl(control: Control): Pending {
  const data = encoder.encode(JSON.stringify(control))
  if (data.length + 1 > MAX_CONTROL_BYTES) throw new Error('File control is too large')
  return pendingBytes(directFileFrame(0, data))
}
export function directFileFrame(kind: 0 | 1, payload: Uint8Array): Uint8Array {
  if (payload.length > DIRECT_FILE_CHUNK_BYTES) throw new Error('File frame is too large')
  const framed = new Uint8Array(payload.length + 5)
  new DataView(framed.buffer).setUint32(0, payload.length + 1, false)
  framed[4] = kind; framed.set(payload, 5)
  return framed
}
export class DirectFileReader {
  private bytes = new Uint8Array(0)
  push(chunk: Uint8Array): Uint8Array[] {
    if (chunk.length > DIRECT_FILE_CHUNK_BYTES + 1024) throw new Error('File read is too large')
    const combined = new Uint8Array(this.bytes.length + chunk.length)
    combined.set(this.bytes); combined.set(chunk, this.bytes.length); this.bytes = combined
    const packets: Uint8Array[] = []
    let consumed = 0
    while (this.bytes.length - consumed >= 4) {
      const length = new DataView(this.bytes.buffer, this.bytes.byteOffset + consumed, 4).getUint32(0, false)
      if (!length || length > DIRECT_FILE_CHUNK_BYTES + 1) throw new Error('Invalid file transfer frame size')
      const end = consumed + 4 + length
      if (end > this.bytes.length) break
      packets.push(this.bytes.slice(consumed + 4, end)); consumed = end
    }
    this.bytes = this.bytes.slice(consumed)
    return packets
  }
}
export function parseControl(packet: Uint8Array): Control {
  if (packet[0] !== 0 || packet.length > MAX_CONTROL_BYTES) throw new Error('Invalid file transfer frame')
  const value = JSON.parse(decoder.decode(packet.subarray(1))) as Record<string, unknown>
  if (!value || Array.isArray(value) || typeof value !== 'object') throw new Error('Invalid file transfer frame')
  const keys = Object.keys(value).sort().join(',')
  const id = typeof value.id === 'string' && /^[a-z\d_-]{1,128}$/i.test(value.id)
  const digest = (v: unknown) => typeof v === 'string' && /^[a-f\d]{64}$/i.test(v)
  const offset = Number.isSafeInteger(value.offset) && Number(value.offset) >= 0
  if ((value.type === 'accept' || value.type === 'decline') && keys === 'id,token,type' && id && digest(value.token)) return value as Control
  if ((value.type === 'done' || value.type === 'received' || value.type === 'cancel') && keys === 'type') return value as Control
  if (value.type === 'resume' && keys === 'id,manifest,offset,prefix_sha256,token,type' && id && digest(value.token) && digest(value.manifest) && digest(value.prefix_sha256) && offset) return value as Control
  if (value.type === 'ready' && keys === 'id,offset,type' && id && offset) return value as Control
  if (value.type === 'offered' && keys === 'id,type' && id) return value as Control
  if (value.type === 'rejected' && keys === 'id,reason,type' && id && ['unavailable', 'mismatch', 'expired'].includes(String(value.reason))) return value as Control
  if (value.type === 'offer' && keys === 'files,id,token,type,version' && id && digest(value.token) && value.version === 2 && Array.isArray(value.files)) {
    const files = value.files as { filename: string; size_bytes: number; sha256: string }[]
    if (files.some(f => !f || Object.keys(f).sort().join(',') !== 'filename,sha256,size_bytes')) throw new Error('Invalid file details')
    validateFiles(files.map(f => ({ filename: f.filename, sizeBytes: f.size_bytes, sha256: f.sha256 })))
    return value as Control
  }
  throw new Error('Invalid file transfer request')
}
/** Canonical manifest: count, then UTF-8 name length/name, u64 size and raw SHA-256. */
export function manifestDigest(files: DirectFileSpec[]): string {
  validateFiles(files)
  const hash = sha256.create()
  const word = new Uint8Array(4); new DataView(word.buffer).setUint32(0, files.length, false); hash.update(word)
  for (const file of files) {
    const name = encoder.encode(file.filename)
    new DataView(word.buffer).setUint32(0, name.length, false); hash.update(word); hash.update(name)
    const size = new Uint8Array(8); new DataView(size.buffer).setBigUint64(0, BigInt(file.sizeBytes), false); hash.update(size)
    hash.update(Uint8Array.from(file.sha256.match(/../g)!, hex => parseInt(hex, 16)))
  }
  return toHex(hash.digest())
}
export async function prefixDigest(files: DirectFileSource[], offset: number, signal?: AbortSignal): Promise<string> {
  const total = files.reduce((n, f) => n + f.sizeBytes, 0)
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > total) throw new Error('Invalid file resume offset')
  const hash = sha256.create()
  for (const file of files) {
    const limit = Math.min(file.sizeBytes, offset)
    for (let start = 0; start < limit; start += DIRECT_FILE_CHUNK_BYTES) {
      signal?.throwIfAborted()
      hash.update(new Uint8Array(await abortable(file.blob.slice(start, Math.min(limit, start + DIRECT_FILE_CHUNK_BYTES)).arrayBuffer(), signal)))
      if (start && start % (1024 * 1024) === 0) await abortable(new Promise<void>(resolve => setTimeout(resolve, 0)), signal)
    }
    offset -= limit
    if (!offset) break
  }
  return toHex(hash.digest())
}
