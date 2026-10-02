'use strict'

const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROWS = Number(process.env.ROWS || 240000)
const RUNS = Number(process.env.RUNS || 5)
const HOT_BUILD_MULTIPLIER = Number(process.env.HOT_BUILD_MULTIPLIER || 1024)

async function request (route, options) {
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

function generateCustomers () {
  const rows = ['id,region']
  for (let copy = 0; copy < HOT_BUILD_MULTIPLIER; copy++) rows.push('1,AMERICA')
  for (let id = 2; id <= 1000; id++) rows.push(`${id},${['AFRICA', 'AMERICA', 'ASIA', 'EUROPE'][id % 4]}`)
  return `${rows.join('\n')}\n`
}

function generateOrders (skewed) {
  const rows = ['id,customer_id,amount']
  const groups = new Map()
  for (let id = 1; id <= ROWS; id++) {
    const customerId = skewed && id % 10 < 8 ? 1 : 1 + ((id * 7919) % 1000)
    const amount = 1 + ((id * 104729) % 10000)
    rows.push(`${id},${customerId},${amount}`)
    const region = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE'][customerId % 4]
    const group = groups.get(region) || { region, orders: 0, total: 0 }
    const matches = customerId === 1 ? HOT_BUILD_MULTIPLIER : 1
    group.orders += matches
    group.total += amount * matches
    groups.set(region, group)
  }
  return { csv: `${rows.join('\n')}\n`, expected: [...groups.values()].sort((a, b) => a.region.localeCompare(b.region)) }
}

async function upload (name, csv) {
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([csv], { type: 'text/csv' }), `${name}.csv`)
  return request('/api/datasets/upload', { method: 'POST', body })
}

function normalize (rows) {
  return rows.map(row => Object.fromEntries(Object.entries(row).map(([key, value]) => [
    key, /^-?\d+(?:\.\d+)?$/.test(String(value)) ? Number(value) : String(value)
  ])))
}

async function execute (ordersId, customersId, skewSplitFactor) {
  const sql = 'SELECT c.region AS region, COUNT(*) AS orders, SUM(o.amount) AS total FROM orders o INNER JOIN customers c ON o.customer_id = c.id GROUP BY c.region ORDER BY region ASC'
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId: ordersId, datasetIds: { customers: customersId }, sql,
      resourceBudget: {
        joinStrategy: 'hash_shuffle', skewSplitFactor, workerLimit: 8,
        maxExecutionMs: 120000, maxResultRows: 1000
      }
    })
  })
  const rows = await new Promise((resolve, reject) => {
    const output = []
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error(`Timed out ${submitted.jobId}`)), 130000)
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
  return { rows, details: await request(`/api/query/jobs/${submitted.jobId}`) }
}

function percentile (values, p) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)]
}

function summarize (runs) {
  return {
    p50Ms: percentile(runs.map(run => Number(run.details.job.execution_time_ms)), 0.5),
    p95Ms: percentile(runs.map(run => Number(run.details.job.execution_time_ms)), 0.95),
    medianCriticalTaskMs: percentile(runs.map(run => Math.max(...run.details.tasks
      .filter(task => task.is_winner)
      .map(task => new Date(task.completed_at) - new Date(task.started_at)))), 0.5),
    medianTransferredBytes: percentile(runs.map(run => run.details.tasks.reduce(
      (sum, task) => sum + Number(task.transferred_bytes || 0), 0
    )), 0.5),
    checksum: runs[0].details.job.result_checksum,
    plan: runs.at(-1).details.job.plan_json
  }
}

async function measure (ordersId, customersId, expected, skewSplitFactor) {
  await execute(ordersId, customersId, skewSplitFactor)
  const runs = []
  for (let index = 0; index < RUNS; index++) {
    const result = await execute(ordersId, customersId, skewSplitFactor)
    if (JSON.stringify(normalize(result.rows)) !== JSON.stringify(expected)) throw new Error('Adaptive join result differed from generated exact result')
    if (runs.length && result.details.job.result_checksum !== runs[0].details.job.result_checksum) throw new Error('Adaptive join checksum was not repeatable')
    runs.push(result)
  }
  return summarize(runs)
}

async function main () {
  const customers = await upload('customers', generateCustomers())
  const skewed = generateOrders(true)
  const balanced = generateOrders(false)
  const [skewedOrders, balancedOrders] = await Promise.all([
    upload('orders', skewed.csv), upload('orders', balanced.csv)
  ])
  const skewStatic = await measure(skewedOrders.datasetId, customers.datasetId, skewed.expected, 100)
  const skewAdaptive = await measure(skewedOrders.datasetId, customers.datasetId, skewed.expected, 1.3)
  const balancedStatic = await measure(balancedOrders.datasetId, customers.datasetId, balanced.expected, 100)
  const balancedAdaptive = await measure(balancedOrders.datasetId, customers.datasetId, balanced.expected, 1.3)

  if (skewStatic.checksum !== skewAdaptive.checksum) throw new Error('Hot-key splitting changed the skewed result checksum')
  if (balancedStatic.checksum !== balancedAdaptive.checksum) throw new Error('Adaptive planning changed the balanced result checksum')
  if (skewStatic.plan.shuffle.hotBuckets.length !== 0) throw new Error('Static skew baseline unexpectedly split a bucket')
  if (skewAdaptive.plan.shuffle.hotBuckets.length === 0 || skewAdaptive.plan.shuffle.logicalPartitions <= skewAdaptive.plan.shuffle.buckets) {
    throw new Error('Adaptive execution did not split the detected hot bucket')
  }
  if (!skewAdaptive.plan.runtimeFeedback || Number(skewAdaptive.plan.runtimeFeedback.executions) < 1) {
    throw new Error('The repeated query did not consume persisted runtime feedback')
  }
  if (skewAdaptive.medianCriticalTaskMs >= skewStatic.medianCriticalTaskMs) {
    throw new Error(`Hot splitting did not shorten the critical task: ${skewStatic.medianCriticalTaskMs}ms -> ${skewAdaptive.medianCriticalTaskMs}ms`)
  }
  if (balancedAdaptive.plan.shuffle.hotBuckets.length !== 0) throw new Error('Balanced data was incorrectly classified as skewed')
  if (balancedAdaptive.p50Ms > balancedStatic.p50Ms * 1.25) {
    throw new Error(`Balanced adaptive regression exceeded 25%: ${balancedStatic.p50Ms}ms -> ${balancedAdaptive.p50Ms}ms`)
  }

  console.log(JSON.stringify({
    status: 'passed', rows: ROWS, hotBuildMultiplicity: HOT_BUILD_MULTIPLIER, partitions: 8, measuredRuns: RUNS,
    correctness: { generatedExactComparison: true, checksumInvariant: true },
    skewed: { static: skewStatic, adaptive: skewAdaptive },
    balanced: { allowedRegression: 0.25, static: balancedStatic, adaptive: balancedAdaptive }
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
