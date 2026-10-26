import { ReceiveFiles, SendFiles } from './files.js'
import { cleanup, emit, OFFER_MS, saveClaim, terminalClaim, type Connection, type Runtime } from './state.js'
import { manifestDigest, parseControl, pendingControl, prefixDigest, validateClaim } from './wire.js'
import type { DirectFileClaim } from './types.js'

function reject(c: Connection, id: string, reason: 'unavailable' | 'mismatch' | 'expired'): boolean {
  c.pending = pendingControl({ type: 'rejected', id, reason })
  c.role = { kind: 'finish' }; c.doneSent = true; c.terminal = true
  return true
}
export async function handlePacket(r: Runtime, c: Connection, packet: Uint8Array): Promise<boolean> {
  if (packet[0] === 1) {
    if (c.role.kind === 'finish') return true
    if (c.role.kind !== 'receiving' || !c.ready) throw new Error('Unexpected file data')
    await c.role.write(packet.subarray(1))
    return true
  }
  const control = parseControl(packet)
  if (c.role.kind === 'finish') return control.type !== 'cancel'
  if (control.type === 'offer' && c.role.kind === 'request') {
    const offer = { id: control.id, token: control.token, version: 2 as const, peer: c.peer,
      files: control.files.map(f => ({ filename: f.filename, sizeBytes: f.size_bytes, sha256: f.sha256 })) }
    const receipt = await r.options.store?.load(`receive/${control.id}`)
    if (receipt?.status === 'completed' && receipt.peer === c.peer && receipt.token === control.token &&
      receipt.manifest === manifestDigest(offer.files) && receipt.expires > r.now()) {
      c.pending = pendingControl({ type: 'received' })
      c.role = { kind: 'finish' }; c.doneSent = true; c.terminal = true
      return true
    }
    if (!r.options.authorizeOffer || !await r.options.authorizeOffer(offer)) return reject(c, control.id, 'unavailable')
    emit(r, { type: 'offered', transferId: control.id, peer: c.peer, offer })
    c.pending = pendingControl({ type: 'offered', id: control.id })
    c.role = { kind: 'finish' }; c.doneSent = true; c.terminal = true
    return true
  }
  if (control.type === 'offered' && c.role.kind === 'announce' && control.id === c.transferId) {
    c.terminal = true
    emit(r, { type: 'offered', transferId: c.transferId, peer: c.peer })
    return false
  }
  if (control.type === 'rejected' && control.id === c.transferId) {
    await terminalClaim(r, c, 'cancelled')
    throw new Error(control.reason === 'mismatch' ? 'File resume did not match the offer' : 'File offer is no longer available')
  }
  if ((control.type === 'accept' || control.type === 'decline' || control.type === 'resume') && c.role.kind === 'request') {
    validateClaim(control.id, control.token)
    const offer = r.offers.get(control.id)
    if (!offer || offer.token !== control.token || !offer.peers.has(c.peer)) return reject(c, control.id, 'unavailable')
    if (offer.expires <= r.now()) return reject(c, control.id, 'expired')
    const manifest = manifestDigest(offer.files)
    let claim: DirectFileClaim | undefined
    if (offer.version === 2) {
      claim = await r.options.store?.load(`send/${control.id}`)
      if (claim && (claim.token !== control.token || claim.manifest !== manifest ||
        claim.peer !== c.peer || claim.status === 'cancelled' || claim.status === 'declined')) return reject(c, control.id, 'unavailable')
      if (claim && claim.expires <= r.now()) return reject(c, control.id, 'expired')
    }
    if ([...r.connections.values()].some(other => other !== c && other.transferId === control.id && !other.terminal)) {
      return reject(c, control.id, 'unavailable')
    }
    let offset = 0
    if (control.type === 'resume') {
      if (offer.version !== 2 || !r.options.store) return reject(c, control.id, 'unavailable')
      const total = offer.files.reduce((n, file) => n + file.sizeBytes, 0)
      if (control.manifest !== manifest || control.offset > total ||
        (claim?.status === 'completed' && control.offset !== total) ||
        control.prefix_sha256 !== await prefixDigest(offer.files, control.offset)) return reject(c, control.id, 'mismatch')
      offset = control.offset; c.version = 2
    } else if (claim) return reject(c, control.id, 'unavailable')
    c.transferId = control.id; c.token = control.token; c.manifest = manifest
    if (offer.version === 2) {
      c.claim = claim ?? { peer: c.peer, token: c.token, manifest, expires: offer.expires, status: 'claimed' }
      if (control.type === 'decline') c.claim.status = 'declined'
      await saveClaim(r, 'send', c.transferId, c.claim)
    }
    if (control.type !== 'resume') r.offers.delete(control.id)
    if (control.type === 'decline') {
      c.terminal = true
      emit(r, { type: 'declined', transferId: c.transferId, peer: c.peer })
      return false
    }
    c.role = new SendFiles(offer.files, offset)
    if (c.version === 2) c.pending = pendingControl({ type: 'ready', id: c.transferId, offset })
    emit(r, { type: 'accepted', transferId: c.transferId, peer: c.peer,
      transferredBytes: offset, totalBytes: c.role.total })
    return true
  }
  if (control.type === 'ready' && c.role.kind === 'receiving' && c.version === 2 && !c.ready &&
    control.id === c.transferId && control.offset === c.role.transferred) {
    c.ready = true
    return true
  }
  if (control.type === 'done' && c.role.kind === 'receiving' && c.ready) {
    const role = c.role
    const files = await role.commit()
    await terminalClaim(r, c, 'completed')
    await cleanup(r, c)
    c.role = { kind: 'finish' }; c.pending = pendingControl({ type: 'received' })
    c.doneSent = true
    const completion = { type: 'completed' as const, transferId: c.transferId, peer: c.peer, files,
      transferredBytes: role.transferred, totalBytes: role.total }
    if (c.version === 2) c.completion = completion
    else { c.terminal = true; emit(r, completion) }
    return true
  }
  if (control.type === 'received' && c.role.kind === 'announce') {
    await terminalClaim(r, c, 'completed'); c.terminal = true
    const total = r.offers.get(c.transferId)!.files.reduce((sum, file) => sum + file.sizeBytes, 0)
    emit(r, { type: 'completed', transferId: c.transferId, peer: c.peer, transferredBytes: total, totalBytes: total, files: [] })
    return false
  }
  if (control.type === 'received' && c.role.kind === 'sending' && c.doneSent) {
    await terminalClaim(r, c, 'completed')
    c.terminal = true
    emit(r, { type: 'completed', transferId: c.transferId, peer: c.peer, files: [],
      transferredBytes: c.role.transferred, totalBytes: c.role.total })
    return false
  }
  if (control.type === 'cancel' && c.role.kind !== 'request') {
    await terminalClaim(r, c, 'cancelled')
    await cleanup(r, c)
    c.terminal = true
    emit(r, { type: 'cancelled', transferId: c.transferId, peer: c.peer })
    return false
  }
  throw new Error('Invalid file transfer request')
}
export async function prepareReceiving(r: Runtime, c: Connection): Promise<void> {
  if (!(c.role instanceof ReceiveFiles)) throw new Error('Invalid file destination')
  if (c.version === 1) { c.pending = pendingControl({ type: 'accept', id: c.transferId, token: c.token }); return }
  if (!r.options.store || !r.options.createReceiveFile) throw new Error('Persistent file transfer storage is unavailable')
  c.ready = false
  const previous = await r.options.store.load(`receive/${c.transferId}`)
  if (previous && (previous.peer !== c.peer || previous.token !== c.token || previous.manifest !== c.manifest ||
    ['cancelled', 'declined'].includes(previous.status) || previous.expires <= r.now())) throw new Error('File offer is no longer available')
  c.claim = previous ?? { peer: c.peer, token: c.token, manifest: c.manifest,
    expires: r.now() + OFFER_MS, status: 'claimed' }
  await saveClaim(r, 'receive', c.transferId, c.claim)
  const prefix = await c.role.restore()
  if (previous?.status === 'completed' && c.role.transferred !== c.role.total) throw new Error('Completed file is no longer available')
  c.pending = pendingControl({ type: 'resume', id: c.transferId, token: c.token,
    manifest: c.manifest, offset: c.role.transferred, prefix_sha256: prefix })
}
