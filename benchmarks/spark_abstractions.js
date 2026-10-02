'use strict'

const WebSocket = require('ws')
const { execFileSync } = require('node:child_process')
const crypto = require('node:crypto')
const path = require('node:path')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROOT = path.resolve(__dirname, '..')

async function request (route, options) {
  const startedAt = Date.now()
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return { payload, durationMs: Date.now() - startedAt }
}

async function upload () {
  const lines = ['id,region,amount,payload']
  for (let id = 1; id <= 60000; id++) {
    const payload = crypto.createHash('sha256').update(`spark-cache-${id}`).digest('hex')
    lines.push(`${id},${['east', 'north', 'south', 'west'][id % 4]},${(id * 7919) % 1000},${payload}`)
  }
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), 'spark_lab.csv')
  return (await request('/api/datasets/upload', { method: 'POST', body })).payload
}

async function uploadCsv (filename, lines, partitionCount) {
  const body = new FormData()
  body.append('partitionCount', String(partitionCount))
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), filename)
  return (await request('/api/datasets/upload', { method: 'POST', body })).payload
}

function percentile (values, quantile) {
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1)]
}

async function main () {
  const dataset = await upload()
  const sql = 'SELECT region, COUNT(*) AS rows, SUM(amount) AS total FROM spark_lab GROUP BY region ORDER BY region ASC'
  const planningTimes = []
  const plans = []
  for (let index = 0; index < 20; index++) {
    const created = await request('/api/plans', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ datasetId: dataset.datasetId, sql, transformations: [
        { operation: 'groupBy', columns: ['region'] }, { operation: 'aggregate', function: 'SUM' }
      ] })
    })
    planningTimes.push(created.durationMs)
    if (created.payload.executed_job_id !== null || created.payload.status !== 'lazy') throw new Error('Lazy plan executed before an action')
    plans.push(created.payload)
  }
  const planningP95Ms = percentile(planningTimes, 0.95)
  if (planningP95Ms >= 50) throw new Error(`Lazy planning p95 exceeded 50ms: ${planningP95Ms}`)

  const budget = { cacheLevel: 'MEMORY_AND_DISK', cacheBudgetBytes: 64 * 1024 * 1024 }
  const cold = (await request(`/api/plans/${plans[0].id}/actions/collect`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: budget })
  })).payload
  const warm = (await request(`/api/plans/${plans[1].id}/actions/count`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: budget })
  })).payload
  const [coldJob, warmJob, accumulators] = await Promise.all([
    request(`/api/query/jobs/${cold.jobId}`).then(value => value.payload),
    request(`/api/query/jobs/${warm.jobId}`).then(value => value.payload),
    request(`/api/query/jobs/${warm.jobId}/accumulators`).then(value => value.payload)
  ])
  if (coldJob.job.result_checksum !== warmJob.job.result_checksum) throw new Error('Cache changed result checksum')
  if (!warmJob.cacheEvents.every(event => event.cache_hit)) throw new Error('Warm action did not reuse every worker partition')

  const cacheLevelEvidence = []
  let planIndex = 4
  for (const cacheLevel of ['MEMORY', 'DISK', 'MEMORY_AND_DISK']) {
    const levelBudget = { cacheLevel, cacheBudgetBytes: 64 * 1024 * 1024 }
    const levelCold = (await request(`/api/plans/${plans[planIndex++].id}/actions/count`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: levelBudget })
    })).payload
    const levelWarm = (await request(`/api/plans/${plans[planIndex++].id}/actions/count`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: levelBudget })
    })).payload
    const [coldEvidence, warmEvidence] = await Promise.all([
      request(`/api/query/jobs/${levelCold.jobId}`).then(value => value.payload),
      request(`/api/query/jobs/${levelWarm.jobId}`).then(value => value.payload)
    ])
    if (coldEvidence.job.result_checksum !== warmEvidence.job.result_checksum ||
        !warmEvidence.cacheEvents.every(event => event.cache_hit)) {
      throw new Error(`${cacheLevel} cache did not preserve checksum and warm hits`)
    }
    cacheLevelEvidence.push({
      cacheLevel, coldHits: coldEvidence.cacheEvents.filter(event => event.cache_hit).length,
      warmHits: warmEvidence.cacheEvents.filter(event => event.cache_hit).length,
      checksum: warmEvidence.job.result_checksum
    })
  }

  const workersBeforeEviction = (await request('/api/workers')).payload
  const evictionsBefore = workersBeforeEviction.reduce((sum, worker) => sum + Number(worker.cache?.evictions || 0), 0)
  await request(`/api/plans/${plans[planIndex++].id}/actions/count`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ resourceBudget: { cacheLevel: 'MEMORY', cacheBudgetBytes: 1024 * 1024, workerLimit: 1 } })
  })
  const workersAfterEviction = (await request('/api/workers')).payload
  const evictionWorkers = workersAfterEviction.filter(worker => Number(worker.cache?.evictions || 0) > 0)
  const evictionsAfter = workersAfterEviction.reduce((sum, worker) => sum + Number(worker.cache?.evictions || 0), 0)
  if (evictionsAfter <= evictionsBefore || evictionWorkers.some(worker => Number(worker.cache.totalBytes) > 1024 * 1024)) {
    throw new Error('Worker cache did not evict to its 1 MiB bound')
  }

  const buildLines = ['id,label']
  for (let id = 1; id <= 200; id++) buildLines.push(`${id},label-${id % 8}`)
  const probeLines = ['id,build_id,amount']
  for (let id = 1; id <= 20000; id++) probeLines.push(`${id},${1 + (id % 200)},${id % 1000}`)
  const [buildDataset, probeDataset] = await Promise.all([
    uploadCsv('spark_build.csv', buildLines, 2),
    uploadCsv('spark_probe.csv', probeLines, 8)
  ])
  const joinSql = 'SELECT b.label AS label, COUNT(*) AS rows, SUM(p.amount) AS total FROM spark_probe p INNER JOIN spark_build b ON p.build_id = b.id GROUP BY b.label ORDER BY label ASC'
  const joinPlans = []
  for (let index = 0; index < 2; index++) {
    joinPlans.push((await request('/api/plans', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        datasetId: probeDataset.datasetId, datasetIds: { spark_build: buildDataset.datasetId },
        sql: joinSql, transformations: [{ operation: 'join' }, { operation: 'aggregate' }]
      })
    })).payload)
  }
  const broadcastBudget = {
    cacheLevel: 'MEMORY_AND_DISK', cacheBudgetBytes: 64 * 1024 * 1024,
    workerLimit: 4, joinStrategy: 'broadcast'
  }
  const broadcastCold = (await request(`/api/plans/${joinPlans[0].id}/actions/collect`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: broadcastBudget })
  })).payload
  const broadcastWarm = (await request(`/api/plans/${joinPlans[1].id}/actions/collect`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ resourceBudget: broadcastBudget })
  })).payload
  const [broadcastColdJob, broadcastWarmJob, workersAfterBroadcast] = await Promise.all([
    request(`/api/query/jobs/${broadcastCold.jobId}`).then(value => value.payload),
    request(`/api/query/jobs/${broadcastWarm.jobId}`).then(value => value.payload),
    request('/api/workers').then(value => value.payload)
  ])
  const broadcastWorkers = new Set(broadcastWarmJob.tasks.filter(task => task.is_winner).map(task => task.worker_id))
  const leakedPins = workersAfterBroadcast.flatMap(worker => worker.cache?.values || []).filter(value => value.refs !== 0)
  if (broadcastColdJob.job.result_checksum !== broadcastWarmJob.job.result_checksum ||
      broadcastWarmJob.job.plan_json.joinStrategy !== 'broadcast' || broadcastWorkers.size < 2 ||
      !broadcastWarmJob.cacheEvents.every(event => event.cache_hit) || leakedPins.length > 0) {
    throw new Error('Broadcast cache was not checksum-safe, reused per worker, or released after the join')
  }
  const rowsAccumulator = accumulators.accumulators.find(item => item.name === 'rows_scanned')
  const passedAccumulator = accumulators.accumulators.find(item => item.name === 'rows_passed_filter')
  const shuffleAccumulator = accumulators.accumulators.find(item => item.name === 'bytes_shuffled_total')
  if (Number(rowsAccumulator?.value) !== 60000 || rowsAccumulator.committed_partitions !== 8 ||
      Number(passedAccumulator?.value) !== 60000 || Number(shuffleAccumulator?.value) !== 0) {
    throw new Error('Winner accumulators did not match scan/filter/shuffle truth')
  }

  await request('/api/chaos', {
    method: 'PUT', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delay', partitionIndex: 0, delayMs: 900, occurrences: 1 })
  })
  const speculative = (await request(`/api/plans/${plans[3].id}/actions/collect`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ resourceBudget: { speculation: true, cacheLevel: 'NONE' } })
  })).payload
  const [speculativeJob, speculativeAccumulators] = await Promise.all([
    request(`/api/query/jobs/${speculative.jobId}`).then(value => value.payload),
    request(`/api/query/jobs/${speculative.jobId}/accumulators`).then(value => value.payload)
  ])
  const speculativeWinners = speculativeJob.tasks.filter(task => task.is_winner)
  const speculativeRows = speculativeAccumulators.accumulators.find(item => item.name === 'rows_scanned')
  const speculativePassed = speculativeAccumulators.accumulators.find(item => item.name === 'rows_passed_filter')
  if (speculativeJob.tasks.length <= speculativeWinners.length || Number(speculativeRows?.value) !== 60000 ||
      Number(speculativePassed?.value) !== 60000 || speculativeRows.committed_partitions !== speculativeWinners.length) {
    throw new Error('Retry/speculation loser attempts inflated winner accumulators')
  }

  const materialized = (await request(`/api/plans/${plans[2].id}/actions/materialize`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'spark_rollup', partitionCount: 2, resourceBudget: budget })
  })).payload
  const lineage = (await request(`/api/lineage?datasetId=${dataset.datasetId}`)).payload
  if (!lineage.nodes.some(node => node.kind === 'transformation') || !lineage.nodes.some(node => node.kind === 'partition')) throw new Error('Lineage DAG omitted operator/partition nodes')
  const logicalPartitionKey = coldJob.tasks.find(task => task.is_winner).logical_partition_key
  await request('/api/lineage/invalidate', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jobId: cold.jobId, logicalPartitionKey, reason: 'seeded replay gate deletion' })
  })
  execFileSync('docker', ['compose', 'kill', '-s', 'SIGKILL', 'worker-1'], { cwd: ROOT, stdio: 'ignore' })
  let recovered
  try {
    recovered = (await request('/api/lineage/recover', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jobId: cold.jobId, logicalPartitionKey })
    })).payload
  } finally {
    execFileSync('docker', ['compose', 'up', '-d', 'worker-1'], { cwd: ROOT, stdio: 'ignore' })
  }
  const recoveryJob = (await request(`/api/query/jobs/${recovered.recoveryJobId}`)).payload
  if (recoveryJob.tasks.filter(task => task.is_winner).length !== 1 || !recovered.partitionChecksumMatch || !recovered.ancestor || recovered.replayPath.length < 2) throw new Error('Lineage recovery was not single-partition/checksum-safe or omitted its replay path')

  console.log(JSON.stringify({
    status: 'passed', rows: 60000, partitions: 8,
    lazyPlanning: { samples: planningTimes.length, p95Ms: planningP95Ms, thresholdMs: 50 },
    cache: { coldMs: coldJob.job.execution_time_ms, warmMs: warmJob.job.execution_time_ms, coldHits: coldJob.cacheEvents.filter(event => event.cache_hit).length, warmHits: warmJob.cacheEvents.filter(event => event.cache_hit).length },
    cacheLevels: cacheLevelEvidence,
    cacheEviction: { evictionsBefore, evictionsAfter, boundedWorkers: evictionWorkers.map(worker => ({ id: worker.id, bytes: worker.cache.totalBytes, evictions: worker.cache.evictions })) },
    broadcastCache: { workers: broadcastWorkers.size, coldHits: broadcastColdJob.cacheEvents.filter(event => event.cache_hit).length, warmHits: broadcastWarmJob.cacheEvents.filter(event => event.cache_hit).length, leakedPins: leakedPins.length, checksum: broadcastWarmJob.job.result_checksum },
    accumulators: accumulators.accumulators,
    speculativeAccumulators: { attempts: speculativeJob.tasks.length, winners: speculativeWinners.length, values: speculativeAccumulators.accumulators },
    lineage: { nodes: lineage.nodes.length, edges: lineage.edges.length, recomputedPartitions: recovered.recomputedPartitions, partitionChecksumMatch: recovered.partitionChecksumMatch, ancestor: recovered.ancestor, replayPath: recovered.replayPath, workerFailure: 'worker-1 SIGKILL before replay' },
    materializedDatasetId: materialized.materializedDataset.datasetId
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
