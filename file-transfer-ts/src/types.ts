// Extracted from Iris Chat (MIT), Copyright Iris Chat contributors.
import type { Config, FipsDatagramEndpoint } from '@fips/tcp'

export interface DirectFileSpec { filename: string; sizeBytes: number; sha256: string }
export interface DirectFileSource extends DirectFileSpec { blob: Blob }
export interface DirectFileResult { filename: string; blob?: Blob }
/** A sink owns only this transfer's staged output. commit must be idempotent. */
export interface DirectFileSink {
  retained?: Blob
  write(chunk: Uint8Array): Promise<void>
  finish(): Promise<Blob | undefined>
  commit?(): Promise<Blob>
  suspend?(): Promise<void>
  abort(): Promise<void>
}
export interface DirectFileClaim {
  peer: string
  token: string
  manifest: string
  expires: number
  status: 'claimed' | 'completed' | 'cancelled' | 'declined'
}
/** save resolves only after the claim is durable. Scope storage to the local identity. */
export interface DirectFileStateStore {
  load(key: string): Promise<DirectFileClaim | undefined>
  save(key: string, claim: DirectFileClaim): Promise<void>
}
export interface DirectFileAnnouncement {
  id: string; token: string; version: 2; peer: string; files: DirectFileSpec[]
}
export interface DirectFileEvent {
  type: 'offered' | 'accepted' | 'progress' | 'completed' | 'declined' | 'cancelled' | 'failed' | 'paused'
  transferId: string
  peer: string
  transferredBytes?: number
  totalBytes?: number
  files?: DirectFileResult[]
  offer?: DirectFileAnnouncement
  error?: string
}
export interface DirectFileTcpOptions {
  endpoint: FipsDatagramEndpoint
  localPeer: string
  onEvent: (event: DirectFileEvent) => void
  onError?: (error: Error) => void
  createReceiveFile?: (transferId: string, index: number, spec: DirectFileSpec) => Promise<DirectFileSink>
  store?: DirectFileStateStore
  /** Authorize an authenticated in-band offer before notifying the application. */
  authorizeOffer?: (offer: DirectFileAnnouncement) => boolean | Promise<boolean>
  tcpConfig?: Partial<Config>
  isnSeed?: bigint | number
  now?: () => number
}
export interface DirectFileTransferOptions { version?: 1 | 2; expires?: number }
