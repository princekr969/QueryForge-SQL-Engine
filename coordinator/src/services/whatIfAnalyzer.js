'use strict'

function predictWhatIf (job, tasks, input = {}) {
  const winners = tasks.filter(task => task.is_winner)
  const currentWorkers = Math.max(1, new Set(winners.map(task => task.worker_id)).size)
  const targetWorkers = input.workers === undefined ? currentWorkers : Number(input.workers)
  if (!Number.isInteger(targetWorkers) || targetWorkers < 1 || targetWorkers > 32) throw new Error('workers must be an integer from 1 to 32')
  const currentBudget = job.resource_budget_json || {}
  const currentCombiner = currentBudget.combiner !== false
  const targetCombiner = input.combiner === undefined ? currentCombiner : input.combiner
  if (typeof targetCombiner !== 'boolean') throw new Error('combiner must be boolean')
  if (input.cacheWarm !== undefined && typeof input.cacheWarm !== 'boolean') throw new Error('cacheWarm must be boolean')
  const cacheWarm = input.cacheWarm === true
  const base = Number(job.execution_time_ms || 0)
  const parallelFraction = 0.85
  let ratio = (1 - parallelFraction) + parallelFraction * (currentWorkers / targetWorkers)
  const transfer = winners.reduce((sum, task) => sum + Number(task.transferred_bytes || 0), 0)
  const scan = winners.reduce((sum, task) => sum + Number(task.bytes_scanned || 0), 0)
  if (targetCombiner !== currentCombiner) ratio *= targetCombiner ? 0.7 : 1.35
  if (cacheWarm && String(currentBudget.cacheLevel || 'NONE') === 'NONE') ratio *= 0.78
  const targetJoinStrategy = input.joinStrategy || job.plan_json?.joinStrategy || null
  if (targetJoinStrategy && !['auto', 'local', 'broadcast', 'hash_shuffle'].includes(targetJoinStrategy)) {
    throw new Error('joinStrategy must be auto, local, broadcast, or hash_shuffle')
  }
  if (input.joinStrategy && input.joinStrategy !== job.plan_json?.joinStrategy) ratio *= 0.9
  return {
    model: 'evidence-weighted Amdahl projection v1',
    baselineExecutionTimeMs: base,
    predictedExecutionTimeMs: Math.max(1, Math.round(base * ratio)),
    predictedSpeedup: ratio > 0 ? 1 / ratio : null,
    inputs: { currentWorkers, targetWorkers, currentCombiner, targetCombiner, cacheWarm, targetJoinStrategy },
    evidence: { winnerTasks: winners.length, measuredTransferBytes: transfer, measuredScanBytes: scan },
    assumptions: ['85% of observed latency is treated as parallelizable.', 'Combiner/cache/join multipliers are directional estimates, not benchmark measurements.', 'No queueing or worker contention is modeled.'],
    confidence: winners.length >= 4 ? 'medium' : 'low'
  }
}

module.exports = { predictWhatIf }
