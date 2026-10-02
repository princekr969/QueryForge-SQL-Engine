'use strict'

function median (values) {
  if (!values.length) return 0
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function buildAutopsy (job, tasks) {
  const attempts = tasks.map(task => {
    const started = task.started_at ? new Date(task.started_at).getTime() : null
    const completed = task.completed_at ? new Date(task.completed_at).getTime() : null
    const durationMs = started !== null && completed !== null ? Math.max(0, completed - started) : null
    const rowsScanned = Number(task.rows_scanned || 0)
    const bytesScanned = Number(task.bytes_scanned || 0)
    return {
      taskId: task.id,
      workerId: task.worker_id,
      status: task.status,
      attempt: Number(task.attempt_number),
      winner: task.is_winner,
      startedAt: task.started_at,
      completedAt: task.completed_at,
      durationMs,
      rowsScanned,
      bytesScanned,
      rowsPerSecond: durationMs > 0 ? rowsScanned * 1000 / durationMs : 0,
      bytesPerSecond: durationMs > 0 ? bytesScanned * 1000 / durationMs : 0,
      bytesTransferred: Number(task.transferred_bytes || 0),
      spilledBytes: Number(task.spilled_bytes || 0),
      peakMemoryBytes: Number(task.peak_memory_bytes || 0),
      cpuTimeMicros: Number(task.cpu_time_micros || 0),
      operator: task.operator_metrics_json?.operator || task.operator_metrics_json?.strategy || 'PartitionScan',
      estimatedRows: Number(task.operator_metrics_json?.estimatedRows || 0),
      actualRows: Number(task.operator_metrics_json?.actualRows || task.rows_processed || 0),
      error: task.error_message
    }
  })
  const completedDurations = attempts.filter(item => item.status === 'completed' && item.durationMs !== null).map(item => item.durationMs)
  const maximum = Math.max(0, ...completedDurations)
  const middle = median(completedDurations)
  const total = field => attempts.reduce((sum, item) => sum + item[field], 0)
  const failed = attempts.filter(item => item.status === 'failed')
  const retried = attempts.filter(item => item.attempt > 1)
  const estimates = attempts.filter(item => item.estimatedRows > 0)
  const meanCardinalityError = estimates.length
    ? estimates.reduce((sum, item) => sum + Math.abs(item.actualRows - item.estimatedRows) / item.estimatedRows, 0) / estimates.length
    : null
  const suggestions = []
  if (middle > 0 && maximum / middle >= 2) suggestions.push(`Observed straggler ratio was ${(maximum / middle).toFixed(2)}× (trigger ≥2×); retain speculation or split the hot partition.`)
  if (failed.length) suggestions.push(`${failed.length} attempt(s) failed (trigger >0); inspect failure lineage before tightening deadlines.`)
  if (total('bytesTransferred') > total('bytesScanned') * 0.5) suggestions.push(`Wire/scan ratio was ${(total('bytesTransferred') / Math.max(1, total('bytesScanned'))).toFixed(2)} (trigger >0.50); add projection, a combiner, or a semi-join filter.`)
  if (total('spilledBytes') > 0) suggestions.push(`${total('spilledBytes')} spill bytes were measured (trigger >0); raise the memory budget or reduce build cardinality.`)
  if (meanCardinalityError !== null && meanCardinalityError > 0.5) suggestions.push(`Mean cardinality error was ${(meanCardinalityError * 100).toFixed(1)}% (trigger >50%); refresh statistics or retain runtime feedback.`)
  if (!suggestions.length) suggestions.push('No dominant pathology detected; the physical plan stayed inside current budgets.')

  const critical = attempts.filter(item => item.durationMs === maximum)[0] || null
  const summedWallMicros = attempts.reduce((sum, item) => sum + Number(item.durationMs || 0) * 1000, 0)
  const costDomains = [
    { name: 'storage', value: total('bytesScanned'), unit: 'bytes', evidence: 'worker bytes read after row-group pruning' },
    { name: 'communication', value: total('bytesTransferred'), unit: 'bytes', evidence: 'serialized worker-to-coordinator payload' },
    { name: 'computation', value: total('cpuTimeMicros'), unit: 'microseconds', evidence: 'worker process CPU time' },
    { name: 'waiting', value: Math.max(0, summedWallMicros - total('cpuTimeMicros')), unit: 'microseconds', evidence: 'task wall time minus measured worker CPU; includes I/O and scheduler wait' },
    { name: 'memory', value: Math.max(0, ...attempts.map(item => item.peakMemoryBytes)), unit: 'bytes', evidence: 'largest task RSS delta' },
    { name: 'spill', value: total('spilledBytes'), unit: 'bytes', evidence: 'bytes spilled under worker memory reservation' },
    { name: 'recomputation', value: failed.reduce((sum, item) => sum + Number(item.cpuTimeMicros || 0), 0), unit: 'microseconds', evidence: 'CPU consumed by failed attempts' }
  ]
  // Compatibility alias for older clients. These are intentionally not ranked:
  // bytes, microseconds, and peak bytes are different dimensions.
  const topCosts = costDomains.slice(0, 3).map(item => ({ ...item, ranked: false }))
  const operatorMap = new Map()
  for (const attempt of attempts) {
    const value = operatorMap.get(attempt.operator) || {
      operator: attempt.operator, attempts: 0, durationMs: 0,
      scannedRows: 0, producedRows: 0, scannedBytes: 0, wireBytes: 0,
      cpuTimeMicros: 0, peakMemoryBytes: 0, spilledBytes: 0
    }
    value.attempts++
    value.durationMs += Number(attempt.durationMs || 0)
    value.scannedRows += attempt.rowsScanned
    value.producedRows += attempt.actualRows
    value.scannedBytes += attempt.bytesScanned
    value.wireBytes += attempt.bytesTransferred
    value.cpuTimeMicros += attempt.cpuTimeMicros
    value.peakMemoryBytes = Math.max(value.peakMemoryBytes, attempt.peakMemoryBytes)
    value.spilledBytes += attempt.spilledBytes
    operatorMap.set(attempt.operator, value)
  }
  const bottleneck = total('spilledBytes') > 0
    ? 'memory_spill'
    : middle > 0 && maximum / middle >= 2
      ? 'straggler'
      : total('bytesTransferred') > total('bytesScanned') * 0.5
        ? 'network_shuffle'
        : total('cpuTimeMicros') / 1000 > Number(job.execution_time_ms || 0) * 2
          ? 'cpu_parallel'
          : 'balanced'
  const operators = [...operatorMap.values()].map(operator => ({
    ...operator,
    rowsPerSecond: operator.durationMs > 0 ? operator.scannedRows * 1000 / operator.durationMs : 0,
    bytesPerSecond: operator.durationMs > 0 ? operator.scannedBytes * 1000 / operator.durationMs : 0
  })).sort((left, right) => right.durationMs - left.durationMs)
  const operatorInstances = attempts.filter(attempt => attempt.durationMs !== null).map(attempt => ({
    operator: attempt.operator,
    taskId: attempt.taskId,
    workerId: attempt.workerId,
    attempt: attempt.attempt,
    winner: attempt.winner,
    durationMs: attempt.durationMs,
    scannedRows: attempt.rowsScanned,
    producedRows: attempt.actualRows,
    scannedBytes: attempt.bytesScanned,
    wireBytes: attempt.bytesTransferred,
    cpuTimeMicros: attempt.cpuTimeMicros,
    peakMemoryBytes: attempt.peakMemoryBytes,
    spilledBytes: attempt.spilledBytes,
    rowsPerSecond: attempt.rowsPerSecond,
    bytesPerSecond: attempt.bytesPerSecond
  })).sort((left, right) => right.durationMs - left.durationMs || left.taskId.localeCompare(right.taskId))
  return {
    jobId: job.id,
    status: job.status,
    checksum: job.result_checksum,
    executionTimeMs: Number(job.execution_time_ms || 0),
    strategy: job.plan_json?.joinStrategy || (job.plan_json?.aggregations?.length ? 'partial_aggregation' : 'partition_scan'),
    criticalPath: critical ? { taskId: critical.taskId, workerId: critical.workerId, durationMs: critical.durationMs, operator: critical.operator } : null,
    totals: {
      attempts: attempts.length,
      failedAttempts: failed.length,
      retriedAttempts: retried.length,
      rowsScanned: total('rowsScanned'),
      bytesScanned: total('bytesScanned'),
      bytesTransferred: total('bytesTransferred'),
      spilledBytes: total('spilledBytes'),
      peakTaskMemoryBytes: Math.max(0, ...attempts.map(item => item.peakMemoryBytes)),
      cpuTimeMicros: total('cpuTimeMicros')
    },
    estimation: { meanRelativeCardinalityError: meanCardinalityError },
    bottleneck,
    costDomains,
    costDomainNote: 'Measured domains use different units and are not ranked against each other.',
    topCosts,
    operators,
    operatorInstances,
    topOperators: operatorInstances.slice(0, 3),
    stragglerRatio: middle > 0 ? maximum / middle : 1,
    suggestions,
    timeline: attempts
  }
}

module.exports = { buildAutopsy }
