'use strict'

const MODES = new Set(['delay', 'network_loss', 'corrupted_input', 'duplicate', 'skew'])

let rule = null

function configure (next) {
  if (!next || !MODES.has(next.mode)) throw new Error(`mode must be one of: ${[...MODES].join(', ')}`)
  const delayMs = Number(next.delayMs || 0)
  if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > 30000) throw new Error('delayMs must be between 0 and 30000')
  const partitionIndex = next.partitionIndex === undefined ? null : Number(next.partitionIndex)
  if (partitionIndex !== null && (!Number.isInteger(partitionIndex) || partitionIndex < 0)) {
    throw new Error('partitionIndex must be a non-negative integer')
  }
  rule = {
    mode: next.mode,
    targetWorker: next.targetWorker || null,
    partitionIndex,
    delayMs,
    remaining: Number.isInteger(next.occurrences) ? Math.max(1, next.occurrences) : 1,
    configuredAt: new Date().toISOString()
  }
  return rule
}

function clear () {
  const previous = rule
  rule = null
  return previous
}

function snapshot () {
  return rule
}

function consume ({ workerId, partitionIndex }) {
  if (!rule || rule.remaining < 1) return null
  if (rule.targetWorker && rule.targetWorker !== workerId) return null
  if (rule.partitionIndex !== null && rule.partitionIndex !== partitionIndex) return null
  const effect = { ...rule }
  rule.remaining--
  if (rule.remaining < 1) rule = null
  return effect
}

module.exports = { configure, clear, snapshot, consume, MODES }
