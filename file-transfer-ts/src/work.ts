import type { DirectFileResult } from './types.js'

const MIB = 1024 * 1024
/** One MiB/s plus one minute, bounded by the authenticated offer's lifetime. */
export function verificationDeadline(now: number, bytes: number, expires: number, passes = 1): number {
  return Math.min(expires, now + 60_000 + Math.ceil(bytes / MIB) * 1000 * passes)
}
export async function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise
  signal.throwIfAborted()
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason)
    signal.addEventListener('abort', abort, { once: true })
    promise.then(value => { signal.removeEventListener('abort', abort); resolve(value) },
      error => { signal.removeEventListener('abort', abort); reject(error) })
  })
}
export interface Work {
  kind: 'prefix' | 'commit'
  deadline: number
  controller: AbortController
  done: boolean
  value?: string | DirectFileResult[]
  error?: unknown
  offset?: number
  expected?: string
}
export function startWork(kind: Work['kind'], deadline: number, run: (signal: AbortSignal) => Promise<NonNullable<Work['value']>>): Work {
  const work: Work = { kind, deadline, controller: new AbortController(), done: false }
  void run(work.controller.signal).then(value => { work.value = value; work.done = true },
    error => { work.error = error; work.done = true })
  return work
}
