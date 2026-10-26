// @vitest-environment node
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { readFileSync } from 'node:fs'
import type { FipsDatagramEndpoint, FipsServiceContext } from '@fips/tcp'
import { sha256 } from '@noble/hashes/sha2.js'
import { DirectFileTcp, manifestDigest, prefixDigest, type DirectFileStateStore, type DirectFileClaim,
  type DirectFileEvent, type DirectFileSink, type DirectFileSource } from '../src/index.js'
import { verificationDeadline } from '../src/work.js'
import { pendingControl, parseControl, toHex } from '../src/wire.js'
const vectors = JSON.parse(readFileSync(new URL('../../protocol/file-transfer-v2-vectors.json', import.meta.url), 'utf8'))
class Store implements DirectFileStateStore {
  claims = new Map<string, DirectFileClaim>()
  async load(key: string) { return this.claims.get(key) }
  async save(key: string, claim: DirectFileClaim) { this.claims.set(key, { ...claim }) }
}
class Disk {
  files = new Map<number, Uint8Array>()
  commits = new Set<number>()
  writes = 0
  async sink(_id: string, index: number): Promise<DirectFileSink> {
    return {
      retained: new Blob([this.files.get(index)?.slice().buffer ?? new ArrayBuffer(0)]),
      write: async data => {
        const previous = this.files.get(index) ?? new Uint8Array(0)
        const next = new Uint8Array(previous.length + data.length)
        next.set(previous); next.set(data, previous.length); this.files.set(index, next); this.writes++
      },
      finish: async () => new Blob([this.files.get(index)?.slice().buffer ?? new ArrayBuffer(0)]),
      commit: async () => { this.commits.add(index); return new Blob([this.files.get(index)?.slice().buffer ?? new ArrayBuffer(0)]) },
      suspend: async () => {},
      abort: async () => { this.files.delete(index); this.commits.delete(index) },
    }
  }
}
class Endpoint implements FipsDatagramEndpoint {
  handlers = new Map<number, (context: FipsServiceContext) => void | Promise<void>>()
  dropped = false
  constructor(readonly id: string) { network.set(id, this) }
  registerService(port: number, handler: (context: FipsServiceContext) => void | Promise<void>) {
    this.handlers.set(port, handler); return () => { this.handlers.delete(port) }
  }
  async sendDatagram(args: { dst: string; srcPort?: number; dstPort: number; payload: Uint8Array }) {
    const remote = network.get(args.dst.slice(2)), handler = remote?.handlers.get(args.dstPort)
    if (!this.dropped && !remote?.dropped && handler) queueMicrotask(() => void handler({
      src: `02${this.id}`, srcPort: args.srcPort ?? 0, dstPort: args.dstPort, payload: args.payload.slice(),
    }))
  }
}
const peers = ['a', 'b', 'c'].map(c => c.repeat(64)), token = '9'.repeat(64)
let network = new Map<string, Endpoint>()
const engines: DirectFileTcp[] = []
function device(index: number, store: Store, disk = new Disk()) {
  const endpoint = new Endpoint(peers[index]), events: DirectFileEvent[] = []
  const engine = new DirectFileTcp({ endpoint, localPeer: endpoint.id, store,
    createReceiveFile: (id, i) => disk.sink(id, i), onEvent: e => events.push(e), isnSeed: index + engines.length + 1 })
  engines.push(engine)
  return { engine, endpoint, events, disk }
}
const source = (name: string, data: Uint8Array): DirectFileSource => ({ filename: name,
  sizeBytes: data.length, sha256: toHex(sha256(data)), blob: new Blob([data.slice().buffer]) })
async function until(check: () => boolean, limit = 8000) {
  for (let ms = 0; ms < limit && !check(); ms += 10) await vi.advanceTimersByTimeAsync(10)
  expect(check()).toBe(true)
}
const event = (events: DirectFileEvent[], type: string) => events.findLast(e => e.type === type)
beforeEach(() => { vi.useFakeTimers(); network = new Map() })
afterEach(async () => { await Promise.all(engines.splice(0).map(e => e.dispose())); vi.useRealTimers() })

it('matches canonical Rust/TypeScript manifest and control vectors', async () => {
  const data = [new TextEncoder().encode('hello'), new Uint8Array(0), Uint8Array.of(0, 1, 255)]
  const files = vectors.manifest.files.map((f: any, i: number) => source(f.filename, data[i]))
  expect(manifestDigest(files)).toBe(vectors.manifest.sha256)
  for (const vector of vectors.prefixes) expect(await prefixDigest(files, vector.offset)).toBe(vector.sha256)
  for (const vector of vectors.controls) {
    const frame = pendingControl(vector.control).bytes
    expect(toHex(frame)).toBe(vector.frame_hex)
    expect(parseControl(frame.subarray(4))).toEqual(vector.control)
  }
})

it('resumes a durable prefix across both process restarts and claims only one receiver', async () => {
  const senderStore = new Store(), receiverStore = new Store(), disk = new Disk()
  const bytes = Uint8Array.from({ length: 400_123 }, (_, i) => i % 251), files = [source('data.bin', bytes)]
  let a = device(0, senderStore), b = device(1, receiverStore, disk)
  a.engine.registerOffer('restart', token, [peers[1], peers[2]], files, { version: 2 })
  await b.engine.receive('restart', token, peers[0], files, { version: 2 })
  await until(() => (disk.files.get(0)?.length ?? 0) >= 65_536)
  a.endpoint.dropped = true
  await Promise.all([a.engine.dispose(), b.engine.dispose()])
  const offset = disk.files.get(0)!.length
  expect(offset).toBeGreaterThan(0); expect(offset).toBeLessThan(bytes.length)
  a = device(0, senderStore); b = device(1, receiverStore, disk)
  a.engine.registerOffer('restart', token, [peers[1], peers[2]], files, { version: 2 })
  const other = device(2, new Store())
  await other.engine.receive('restart', token, peers[0], files, { version: 2 })
  await until(() => !!event(other.events, 'failed'))
  await b.engine.receive('restart', token, peers[0], files, { version: 2 })
  await until(() => !!event(a.events, 'completed') && !!event(b.events, 'completed'))
  expect(event(a.events, 'accepted')?.transferredBytes).toBe(offset)
  expect(disk.files.get(0)).toEqual(bytes)
  expect(senderStore.claims.get('send/restart')?.status).toBe('completed')
  expect(receiverStore.claims.get('receive/restart')?.status).toBe('completed')
  await vi.advanceTimersByTimeAsync(100)
  const writes = disk.writes
  await b.engine.receive('restart', token, peers[0], files, { version: 2 })
  await until(() => b.events.filter(e => e.type === 'completed').length === 2)
  expect(disk.writes).toBe(writes)
  expect(disk.commits.size).toBe(1)
})

it('rejects a corrupted retained prefix without emitting additional source bytes', async () => {
  const files = [source('secret.bin', new TextEncoder().encode('private bytes'))]
  const a = device(0, new Store()), disk = new Disk()
  disk.files.set(0, new TextEncoder().encode('wrong'))
  const b = device(1, new Store(), disk)
  a.engine.registerOffer('corrupt', token, [peers[1]], files, { version: 2 })
  await b.engine.receive('corrupt', token, peers[0], files, { version: 2 })
  await until(() => !!event(b.events, 'failed'))
  expect(event(a.events, 'accepted')).toBeUndefined()
  expect(disk.files.size).toBe(0)
})

it('a persisted cancellation cannot be reopened after restart', async () => {
  const store = new Store(), files = [source('small.txt', new Uint8Array([1, 2, 3]))]
  let a = device(0, store)
  a.engine.registerOffer('cancelled', token, [peers[1]], files, { version: 2 })
  await a.engine.cancel('cancelled'); await a.engine.dispose()
  a = device(0, store)
  a.engine.registerOffer('cancelled', token, [peers[1]], files, { version: 2 })
  const b = device(1, new Store())
  await b.engine.receive('cancelled', token, peers[0], files, { version: 2 })
  await until(() => !!event(b.events, 'failed'))
  expect(event(a.events, 'accepted')).toBeUndefined()
})

it('replays an authenticated completed receipt to the sender without rewriting files', async () => {
  const files = [source('hello.txt', new TextEncoder().encode('hello')), source('empty', new Uint8Array()), source('tail', Uint8Array.of(1, 2, 3))]
  const senderStore = new Store(), receiverStore = new Store(), disk = new Disk()
  let a = device(0, senderStore), b = device(1, receiverStore, disk)
  a.engine.registerOffer('receipt', token, [peers[1]], files, { version: 2 })
  await b.engine.receive('receipt', token, peers[0], files, { version: 2 })
  await until(() => !!event(a.events, 'completed') && !!event(b.events, 'completed'))
  await Promise.all([a.engine.dispose(), b.engine.dispose()])
  senderStore.claims.set('send/receipt', { ...senderStore.claims.get('send/receipt')!, status: 'claimed' })
  const writes = disk.writes
  a = device(0, senderStore); b = device(1, receiverStore, disk)
  a.engine.registerOffer('receipt', token, [peers[1]], files, { version: 2 })
  await a.engine.resumeOffer('receipt', peers[1])
  await until(() => !!event(a.events, 'completed'))
  expect(senderStore.claims.get('send/receipt')!.status).toBe('completed')
  expect(disk.writes).toBe(writes)
  expect(b.events).toEqual([])
})

it('does not reveal a completed receipt to another authenticated peer', async () => {
  const files = [source('hello.txt', new TextEncoder().encode('hello'))]
  const receiverStore = new Store()
  receiverStore.claims.set('receive/private', { peer: peers[0], token, manifest: manifestDigest(files),
    expires: Date.now() + 60_000, status: 'completed' })
  const b = device(1, receiverStore), stranger = device(2, new Store())
  await stranger.engine.announceOffer('private', token, peers[1], files)
  await until(() => !!event(stranger.events, 'failed'))
  expect(event(stranger.events, 'completed')).toBeUndefined()
  expect(b.events).toEqual([])
})


it('bounds verification time by retained bytes and authenticated offer expiry', () => {
  expect(verificationDeadline(1000, 32 * 1024 * 1024, 200_000)).toBe(93_000)
  expect(verificationDeadline(1000, 100 * 1024 * 1024 * 1024, 90_000)).toBe(90_000)
})

it('keeps other transfers responsive while a slow resumed prefix exceeds the normal idle timeout', async () => {
  const prefix = new Uint8Array(32 * 1024 * 1024).fill(37)
  const bytes = new Uint8Array(prefix.length + 3); bytes.set(prefix); bytes.set([1, 2, 3], prefix.length)
  const file = source('slow.bin', bytes), disk = new Disk(); disk.files.set(0, prefix)
  let release!: () => void, started = false
  const blocked = new Promise<void>(resolve => { release = resolve })
  const original = file.blob.slice.bind(file.blob)
  file.blob.slice = (start, end, type) => {
    const chunk = original(start, end, type)
    if (!started) {
      started = true
      const read = chunk.arrayBuffer.bind(chunk)
      chunk.arrayBuffer = async () => { await blocked; return read() }
    }
    return chunk
  }
  const a = device(0, new Store()), b = device(1, new Store(), disk)
  a.engine.registerOffer('slow', token, [peers[1]], [file], { version: 2 })
  const receiving = b.engine.receive('slow', token, peers[0], [file], { version: 2 })
  await until(() => started)
  await receiving
  const c = device(2, new Store()), quick = [source('quick.txt', Uint8Array.of(9))]
  a.engine.registerOffer('quick', token, [peers[2]], quick, { version: 2 })
  await c.engine.receive('quick', token, peers[0], quick, { version: 2 })
  await until(() => !!event(c.events, 'completed'))
  await vi.advanceTimersByTimeAsync(61_000)
  expect(b.events.some(e => e.type === 'failed' || e.type === 'paused')).toBe(false)
  release()
  await until(() => !!event(b.events, 'completed'))
  expect(disk.files.get(0)?.length).toBe(bytes.length)
  expect(toHex(sha256(disk.files.get(0)!))).toBe(file.sha256)
}, 15_000)

it('cancels a blocked authenticated prefix job without waiting for the read', async () => {
  const file = source('blocked.bin', Uint8Array.of(1, 2, 3)), disk = new Disk()
  disk.files.set(0, Uint8Array.of(1))
  let started = false
  const original = file.blob.slice.bind(file.blob)
  file.blob.slice = (start, end, type) => {
    const chunk = original(start, end, type)
    chunk.arrayBuffer = () => { started = true; return new Promise(() => {}) }
    return chunk
  }
  const senderStore = new Store(), a = device(0, senderStore), b = device(1, new Store(), disk)
  a.engine.registerOffer('blocked', token, [peers[1]], [file], { version: 2 })
  await b.engine.receive('blocked', token, peers[0], [file], { version: 2 })
  await until(() => started)
  await a.engine.cancel('blocked')
  await until(() => !!event(b.events, 'cancelled'))
  expect(senderStore.claims.get('send/blocked')?.status).toBe('cancelled')
})

it('allows bounded verification/export after Done without blocking unrelated transfers', async () => {
  const bytes = new Uint8Array(16 * 1024 * 1024).fill(19)
  const file = source('export.bin', bytes), disk = new Disk(); disk.files.set(0, bytes)
  let release!: () => void, exporting = false
  const blocked = new Promise<void>(resolve => { release = resolve }), sink = disk.sink.bind(disk)
  disk.sink = async (id, index) => {
    const value = await sink(id, index), commit = value.commit!
    value.commit = async () => { exporting = true; await blocked; return commit() }
    return value
  }
  const a = device(0, new Store()), b = device(1, new Store(), disk)
  a.engine.registerOffer('export', token, [peers[1]], [file], { version: 2 })
  const receiving = b.engine.receive('export', token, peers[0], [file], { version: 2 })
  await until(() => exporting); await receiving
  const c = device(2, new Store()), quick = [source('quick.txt', Uint8Array.of(1))]
  b.engine.registerOffer('quick-export', token, [peers[2]], quick, { version: 2 })
  await c.engine.receive('quick-export', token, peers[1], quick, { version: 2 })
  await until(() => !!event(c.events, 'completed'))
  await vi.advanceTimersByTimeAsync(61_000)
  expect(a.events.some(e => e.type === 'failed' || e.type === 'paused')).toBe(false)
  release()
  await until(() => !!event(a.events, 'completed'))
  expect(disk.commits.size).toBe(1)
}, 15_000)
