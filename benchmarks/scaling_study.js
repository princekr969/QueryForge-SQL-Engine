'use strict'

const { execFileSync } = require('node:child_process')
const path = require('node:path')
const WebSocket = require('ws')

const ROOT = path.resolve(__dirname, '..')
const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROWS = Number(process.env.ROWS || 600000)
const RUNS = Number(process.env.RUNS || 5)
const SCALE = [1, 2, 4, 8]

async function request (route, options) {
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

function generateLineitem () {
  const lines = ['id,return_flag,line_status,quantity,extended_price,discount,revenue,ship_day']
  const expected = new Map()
  let selectiveRevenue = 0
  for (let id = 1; id <= ROWS; id++) {
    const returnFlag = ['A', 'N', 'R'][id % 3]
    const lineStatus = ['F', 'O'][id % 2]
    const quantity = 1 + ((id * 17) % 50)
    const extendedPrice = 100 + ((id * 7919) % 100000)
    const discount = (id * 13) % 11
    const revenue = Math.round(extendedPrice * (100 - discount))
    const shipDay = 1 + ((id * 29) % 2557)
    lines.push(`${id},${returnFlag},${lineStatus},${quantity},${extendedPrice},${discount},${revenue},${shipDay}`)
    const key = returnFlag
    const group = expected.get(key) || {
      return_flag: returnFlag, lines: 0, quantity: 0,
      revenue: 0, min_price: Infinity, max_price: -Infinity
    }
    group.lines++
    group.quantity += quantity
    group.revenue += revenue
    group.min_price = Math.min(group.min_price, extendedPrice)
    group.max_price = Math.max(group.max_price, extendedPrice)
    expected.set(key, group)
    if (shipDay >= 2400 && discount >= 5 && discount <= 7 && quantity < 24) selectiveRevenue += revenue
  }
  return {
    csv: `${lines.join('\n')}\n`,
    expected: {
      q1: [...expected.values()].sort((a, b) => a.return_flag.localeCompare(b.return_flag)),
      q6: [{ revenue: selectiveRevenue }]
    }
  }
}

async function upload (csv) {
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([csv], { type: 'text/csv' }), 'lineitem.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function execute (datasetId, sql, workerLimit) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId, sql,
      resourceBudget: { workerLimit, maxExecutionMs: 120000, maxResultRows: 1000 }
    })
  })
  const rows = await new Promise((resolve, reject) => {
    const output = []
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => {
      socket.terminate()
      reject(new Error(`Timed out ${submitted.jobId}`))
    }, 130000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'row') output.push(event.data)
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete') resolve(output)
      if (event.type === 'complete' || event.type === 'error') {
        clearTimeout(timeout)
        socket.close()
      }
    })
    socket.on('error', reject)
  })
  const details = await request(`/api/query/jobs/${submitted.jobId}`)
  return { rows, details }
}

function normalize (rows) {
  return rows.map(row => Object.fromEntries(Object.entries(row).map(([key, value]) => {
    if (value === null) return [key, null]
    return [key, typeof value === 'number' || /^-?\d+(?:\.\d+)?$/.test(String(value)) ? Number(value) : String(value)]
  })))
}

function percentile (values, p) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)]
}

async function waitForWorkers (count) {
  const deadline = Date.now() + 60000
  while (Date.now() < deadline) {
    const workers = await request('/api/workers')
    const active = workers.filter(worker => worker.liveStatus === 'active')
    if (active.length >= count) return active
    await new Promise(resolve => setTimeout(resolve, 1000))
  }
  throw new Error(`Timed out waiting for ${count} active workers`)
}

async function measureWorkload (datasetId, workload, expected) {
  const measurements = []
  let referenceChecksum = null
  for (const workers of SCALE) {
    await execute(datasetId, workload.sql, workers)
    const samples = []
    for (let run = 0; run < RUNS; run++) {
      const result = await execute(datasetId, workload.sql, workers)
      if (JSON.stringify(normalize(result.rows)) !== JSON.stringify(expected)) {
        throw new Error(`${workload.name} returned an incorrect result with ${workers} worker(s)`)
      }
      const checksum = result.details.job.result_checksum
      if (referenceChecksum && checksum !== referenceChecksum) throw new Error(`${workload.name} checksum changed across scales`)
      referenceChecksum = checksum
      const winners = result.details.tasks.filter(task => task.is_winner)
      samples.push({
        durationMs: Number(result.details.job.execution_time_ms),
        scannedBytes: winners.reduce((sum, task) => sum + Number(task.bytes_scanned || 0), 0),
        transferredBytes: winners.reduce((sum, task) => sum + Number(task.transferred_bytes || 0), 0),
        peakTaskMemoryBytes: Math.max(...winners.map(task => Number(task.peak_memory_bytes || 0)), 0),
        cpuTimeMicros: winners.reduce((sum, task) => sum + Number(task.cpu_time_micros || 0), 0),
        workersUsed: new Set(winners.map(task => task.worker_id)).size,
        checksum
      })
    }
    measurements.push({
      workers,
      repetitions: samples.length,
      p50Ms: percentile(samples.map(sample => sample.durationMs), 0.5),
      p95Ms: percentile(samples.map(sample => sample.durationMs), 0.95),
      medianScannedBytes: percentile(samples.map(sample => sample.scannedBytes), 0.5),
      medianTransferredBytes: percentile(samples.map(sample => sample.transferredBytes), 0.5),
      medianPeakTaskMemoryBytes: percentile(samples.map(sample => sample.peakTaskMemoryBytes), 0.5),
      medianCpuTimeMicros: percentile(samples.map(sample => sample.cpuTimeMicros), 0.5),
      workersUsed: Math.max(...samples.map(sample => sample.workersUsed)),
      checksum: referenceChecksum
    })
  }
  const oneWorker = measurements[0].p50Ms
  for (const measurement of measurements) {
    measurement.speedup = oneWorker / measurement.p50Ms
    measurement.parallelEfficiency = oneWorker / (measurement.workers * measurement.p50Ms)
  }
  return { name: workload.name, sql: workload.sql, checksum: referenceChecksum, measurements }
}

async function main () {
  execFileSync('docker', ['compose', '--profile', 'scaling', 'up', '-d', '--no-deps', '--scale', 'worker-scale=5', 'worker-scale'], {
    cwd: ROOT, stdio: 'pipe'
  })
  const activeWorkers = await waitForWorkers(8)
  const generated = generateLineitem()
  const uploaded = await upload(generated.csv)
  if (uploaded.partitionCount !== 8) throw new Error(`Expected 8 partitions, received ${uploaded.partitionCount}`)
  const workloads = [
    {
      name: 'TPC-H-derived Q1 grouped scan',
      sql: 'SELECT return_flag, COUNT(*) AS lines, SUM(quantity) AS quantity, SUM(revenue) AS revenue, MIN(extended_price) AS min_price, MAX(extended_price) AS max_price FROM lineitem GROUP BY return_flag ORDER BY return_flag ASC',
      expected: generated.expected.q1
    },
    {
      name: 'TPC-H-derived Q6 selective revenue',
      sql: 'SELECT SUM(revenue) AS revenue FROM lineitem WHERE ship_day >= 2400 AND discount >= 5 AND discount <= 7 AND quantity < 24',
      expected: generated.expected.q6
    }
  ]
  const results = []
  for (const workload of workloads) results.push(await measureWorkload(uploaded.datasetId, workload, workload.expected))
  console.log(JSON.stringify({
    status: 'passed', seed: 'deterministic modular TPC-H-derived generator', rows: ROWS,
    datasetId: uploaded.datasetId, partitions: uploaded.partitionCount,
    provisionedWorkers: activeWorkers.length, warmupsPerPoint: 1, measuredRunsPerPoint: RUNS,
    results
  }, null, 2))
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exit(1)
})
