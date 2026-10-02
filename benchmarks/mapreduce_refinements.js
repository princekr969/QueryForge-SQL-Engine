'use strict'

const WebSocket = require('ws')
const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROWS = Number(process.env.ROWS || 30000)
const RUNS = Number(process.env.RUNS || 3)
const STRAGGLER_RUNS = Number(process.env.STRAGGLER_RUNS || 5)

async function request (route, options) {
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

function generateCsv () {
  const lines = ['id,region,category,amount']
  const regions = ['east', 'north', 'south', 'west']
  for (let id = 1; id <= ROWS; id++) lines.push(`${id},${regions[id % 4]},c${id % 40},${1 + ((id * 7919) % 1000)}`)
  return `${lines.join('\n')}\n`
}

async function upload () {
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([generateCsv()], { type: 'text/csv' }), 'mapreduce_lab.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function execute (datasetId, sql, resourceBudget = {}) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ datasetId, sql, resourceBudget: { maxExecutionMs: 120000, ...resourceBudget } })
  })
  const rows = await new Promise((resolve, reject) => {
    const output = []
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error(`Timed out ${submitted.jobId}`)), 125000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'row') output.push(event.data)
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete') resolve(output)
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timeout); socket.close() }
    })
    socket.on('error', reject)
  })
  const details = await request(`/api/query/jobs/${submitted.jobId}`)
  return {
    jobId: submitted.jobId, rows, checksum: details.job.result_checksum, latencyMs: Number(details.job.execution_time_ms),
    transferredBytes: details.tasks.filter(task => task.is_winner).reduce((sum, task) => sum + Number(task.transferred_bytes || 0), 0)
  }
}

function percentile (values, quantile) {
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1)]
}

async function compareCombiner (datasetId, sql) {
  await execute(datasetId, sql, { combiner: true })
  const enabled = []
  const disabled = []
  for (let index = 0; index < RUNS; index++) {
    enabled.push(await execute(datasetId, sql, { combiner: true }))
    disabled.push(await execute(datasetId, sql, { combiner: false }))
  }
  if (new Set([...enabled, ...disabled].map(run => run.checksum)).size !== 1) throw new Error(`Combiner changed query result: ${sql}`)
  const enabledBytes = percentile(enabled.map(run => run.transferredBytes), 0.5)
  const disabledBytes = percentile(disabled.map(run => run.transferredBytes), 0.5)
  if (enabledBytes >= disabledBytes) throw new Error(`Combiner did not reduce real transfer: ${enabledBytes} >= ${disabledBytes}`)
  return {
    sql, checksum: enabled[0].checksum,
    enabled: { p50Ms: percentile(enabled.map(run => run.latencyMs), 0.5), medianTransferredBytes: enabledBytes, evidenceJobId: enabled[0].jobId },
    disabled: { p50Ms: percentile(disabled.map(run => run.latencyMs), 0.5), medianTransferredBytes: disabledBytes, evidenceJobId: disabled[0].jobId },
    transferReduction: 1 - (enabledBytes / disabledBytes)
  }
}

async function measureStraggler (datasetId, sql, speculation) {
  const runs = []
  for (let index = 0; index < STRAGGLER_RUNS; index++) {
    await request('/api/chaos', {
      method: 'PUT', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'delay', partitionIndex: 0, delayMs: 900, occurrences: 1 })
    })
    runs.push(await execute(datasetId, sql, { speculation }))
  }
  const latencies = runs.map(run => run.latencyMs)
  return {
    p50Ms: percentile(latencies, 0.50),
    p95Ms: percentile(latencies, 0.95),
    p99Ms: percentile(latencies, 0.99),
    checksum: runs[0].checksum
  }
}

async function main () {
  const dataset = await upload()
  const workloads = [
    'SELECT region, COUNT(*) AS rows, SUM(amount) AS total, AVG(amount) AS average FROM mapreduce_lab GROUP BY region ORDER BY region ASC',
    'SELECT category, COUNT(*) AS rows, MIN(amount) AS minimum, MAX(amount) AS maximum FROM mapreduce_lab GROUP BY category ORDER BY category ASC',
    'SELECT COUNT(*) AS rows, SUM(amount) AS total, AVG(amount) AS average FROM mapreduce_lab WHERE amount >= 250'
  ]
  const combiner = []
  for (const sql of workloads) combiner.push(await compareCombiner(dataset.datasetId, sql))
  const [balanced, skewed, explain] = await Promise.all([
    request('/api/explain/partition', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ datasetId: dataset.datasetId, column: 'id', buckets: 8 }) }),
    request('/api/explain/partition', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ datasetId: dataset.datasetId, column: 'region', buckets: 8 }) }),
    request('/api/explain', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ datasetId: dataset.datasetId, sql: workloads[0], jobId: combiner[0].disabled.evidenceJobId, resourceBudget: { combiner: false } }) })
  ])
  if (balanced.totalRows !== ROWS || skewed.totalRows !== ROWS) throw new Error('Partition explorer lost rows')
  if (balanced.distribution.reduce((sum, bucket) => sum + bucket.estimatedBytes, 0) !== balanced.totalInputBytes) throw new Error('Partition explorer byte accounting did not reconcile')
  if (balanced.coefficientOfVariation > 0.1) throw new Error(`Integer hash was unexpectedly imbalanced: ${balanced.coefficientOfVariation}`)
  if (skewed.hotBuckets.length === 0) throw new Error('Partition explorer did not expose low-cardinality skew')
  if (explain.mapreduce.combiner !== false || !explain.cost_model.equation ||
      !explain.measured_cost?.input_bytes || !explain.measured_cost?.cpu_time_micros ||
      !explain.measured_cost?.critical_path_ms || !explain.measured_cost?.equations?.communication ||
      !explain.operator_dag.nodes.some(node => node.operator === 'LocalCombiner' && node.enabled === false)) {
    throw new Error('EXPLAIN omitted MapReduce controls or measured teaching costs')
  }
  const baseline = await measureStraggler(dataset.datasetId, workloads[0], false)
  const speculative = await measureStraggler(dataset.datasetId, workloads[0], true)
  if (baseline.checksum !== speculative.checksum) throw new Error('Speculation changed the committed result')
  if (speculative.p99Ms >= baseline.p99Ms) throw new Error(`Speculation did not improve injected p99: ${baseline.p99Ms} -> ${speculative.p99Ms}`)
  console.log(JSON.stringify({
    status: 'passed', rows: ROWS, partitions: 8, measuredRuns: RUNS, stragglerRuns: STRAGGLER_RUNS,
    combiner, partitionExplorer: { balanced, skewed }, explain: { mapreduce: explain.mapreduce, costModel: explain.cost_model },
    straggler: { baseline, speculative, p99Improvement: 1 - (speculative.p99Ms / baseline.p99Ms) }
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
