// Extracted from Iris Chat (MIT), Copyright Iris Chat contributors.
import { abortable } from './work.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { DIRECT_FILE_CHUNK_BYTES, toHex } from './wire.js'
import type { DirectFileResult, DirectFileSink, DirectFileSource, DirectFileSpec, DirectFileTcpOptions } from './types.js'

export class SendFiles {
  readonly kind = 'sending'
  index = 0
  offset = 0
  transferred: number
  readonly total: number
  constructor(readonly files: DirectFileSource[], offset = 0) {
    this.total = files.reduce((n, f) => n + f.sizeBytes, 0)
    this.transferred = offset
    while (this.index < files.length && offset >= files[this.index].sizeBytes) {
      offset -= files[this.index++].sizeBytes
    }
    this.offset = offset
  }
  async nextChunk(): Promise<Uint8Array | undefined> {
    while (this.index < this.files.length) {
      const file = this.files[this.index]
      if (this.offset === file.sizeBytes) { this.index++; this.offset = 0; continue }
      const length = Math.min(DIRECT_FILE_CHUNK_BYTES, file.sizeBytes - this.offset)
      const data = new Uint8Array(await file.blob.slice(this.offset, this.offset + length).arrayBuffer())
      if (data.length !== length) throw new Error('Selected file changed')
      this.offset += length; this.transferred += length
      return data
    }
  }
}

export class ReceiveFiles {
  readonly kind = 'receiving'
  index = 0
  offset = 0
  transferred = 0
  readonly total: number
  readonly sinks: DirectFileSink[] = []
  readonly results: DirectFileResult[] = []
  committed = false
  private hash = sha256.create()
  private prefix = sha256.create()
  private cleaned = false
  constructor(readonly id: string, readonly files: DirectFileSpec[], private readonly options: DirectFileTcpOptions) {
    this.total = files.reduce((n, f) => n + f.sizeBytes, 0)
  }
  async restore(signal?: AbortSignal): Promise<string> {
    let incomplete = false
    for (const [index, file] of this.files.entries()) {
      signal?.throwIfAborted()
      const sink = await this.sink(index)
      signal?.throwIfAborted()
      if (!sink.suspend) throw new Error('This destination cannot resume file transfers')
      const retained = sink.retained
      if ((retained?.size ?? 0) > file.sizeBytes || (incomplete && retained?.size)) throw new Error('Invalid retained file prefix')
      if (incomplete) continue
      const count = retained?.size ?? 0
      if (retained) for (let offset = 0; offset < count; offset += DIRECT_FILE_CHUNK_BYTES) {
        signal?.throwIfAborted()
        const bytes = new Uint8Array(await abortable(retained.slice(offset, offset + DIRECT_FILE_CHUNK_BYTES).arrayBuffer(), signal))
        if (offset && offset % (1024 * 1024) === 0) await abortable(new Promise<void>(resolve => setTimeout(resolve, 0)), signal)
        this.hash.update(bytes); this.prefix.update(bytes)
      }
      this.offset = count; this.transferred += count
      if (count === file.sizeBytes) await this.advance()
      else incomplete = true
    }
    return toHex(this.prefix.clone().digest())
  }
  private async sink(index: number): Promise<DirectFileSink> {
    if (!this.sinks[index]) this.sinks[index] = this.options.createReceiveFile
      ? await this.options.createReceiveFile(this.id, index, this.files[index]) : new MemorySink()
    return this.sinks[index]
  }
  private async advance(): Promise<void> {
    while (this.index < this.files.length) {
      const file = this.files[this.index]
      if (this.offset !== file.sizeBytes) break
      if (toHex(this.hash.clone().digest()) !== file.sha256.toLowerCase()) throw new Error('Received file did not match the offer')
      const sink = await this.sink(this.index)
      const blob = await sink.finish()
      if (blob ? blob.size !== file.sizeBytes : !sink.commit) throw new Error('Received file was not saved completely')
      this.results[this.index] = { filename: file.filename, blob }
      this.index++; this.offset = 0; this.hash = sha256.create()
    }
  }
  async write(data: Uint8Array): Promise<void> {
    if (!data.length || data.length > DIRECT_FILE_CHUNK_BYTES) throw new Error('Invalid file data frame')
    await this.advance()
    while (data.length) {
      const file = this.files[this.index]
      if (!file) throw new Error('Received more data than offered')
      const count = Math.min(data.length, file.sizeBytes - this.offset)
      const chunk = data.subarray(0, count)
      await (await this.sink(this.index)).write(chunk)
      this.hash.update(chunk); this.prefix.update(chunk)
      this.offset += count; this.transferred += count
      data = data.subarray(count)
      await this.advance()
    }
  }
  async commit(signal?: AbortSignal): Promise<DirectFileResult[]> {
    await this.advance()
    if (this.index !== this.files.length) throw new Error('File transfer ended before all files arrived')
    for (const [index, sink] of this.sinks.entries()) {
      signal?.throwIfAborted()
      if (sink.commit) this.results[index].blob = await sink.commit(signal)
      if (this.results[index].blob?.size !== this.files[index].sizeBytes) throw new Error('Received file was not saved completely')
    }
    signal?.throwIfAborted()
    this.committed = true
    return this.results
  }
  async cleanup(preserve: boolean): Promise<void> {
    if (this.cleaned || this.committed) return
    this.cleaned = true
    if (preserve) {
      for (const sink of this.sinks) await sink.suspend?.()
    } else await Promise.allSettled(this.sinks.map(sink => sink.abort()))
    this.hash.destroy(); this.prefix.destroy()
  }
}
class MemorySink implements DirectFileSink {
  private parts: BlobPart[] = []
  async write(chunk: Uint8Array): Promise<void> { this.parts.push(chunk.slice().buffer) }
  async finish(): Promise<Blob> { const blob = new Blob(this.parts); this.parts = []; return blob }
  async abort(): Promise<void> { this.parts = [] }
}
