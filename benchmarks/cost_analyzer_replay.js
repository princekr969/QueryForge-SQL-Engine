'use strict'

const WebSocket = require('ws')
const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'

async function request (route, options) {
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function upload () {
  const lines = ['id,region,category,amount']
  for (let id = 1; id <= 40000; id++) lines.push(`${id},${['east', 'north', 'south', 'west'][id % 4]},c${id % 50},${(id * 3571) % 1000}`)
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), 'cost_lab.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function execute (datasetId, sql) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ datasetId, sql })
  })
  await new Promise((resolve, reject) => {
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error('Query timed out')), 30000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'complete') resolve()
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timeout); socket.close() }
    })
    socket.on('error', reject)
  })
  return submitted.jobId
}

async function main () {
  const dataset = await upload()
  const queries = [
    'SELECT region, COUNT(*) AS rows, SUM(amount) AS total FROM cost_lab GROUP BY region ORDER BY region ASC',
    'SELECT category, COUNT(*) AS rows, AVG(amount) AS average FROM cost_lab GROUP BY category ORDER BY category ASC',
    'SELECT COUNT(*) AS rows, SUM(amount) AS total FROM cost_lab WHERE amount >= 900',
    'SELECT id, region, amount FROM cost_lab WHERE amount >= 990 ORDER BY id ASC LIMIT 100',
    'SELECT region, MIN(amount) AS minimum, MAX(amount) AS maximum FROM cost_lab GROUP BY region ORDER BY region ASC'
  ]
  const jobs = []
  const autopsies = []
  const whatIf = []
  for (const sql of queries) {
    const jobId = await execute(dataset.datasetId, sql)
    jobs.push(jobId)
    const autopsy = await request(`/api/query/jobs/${jobId}/autopsy`)
    if (autopsy.costDomains.length !== 7 || !/not ranked/.test(autopsy.costDomainNote) ||
        autopsy.topOperators.length !== 3 || !autopsy.bottleneck ||
        autopsy.topOperators.some(operator => !operator.taskId || operator.durationMs == null)) {
      throw new Error('Autopsy omitted measured cost attribution or ranked physical-operator evidence')
    }
    autopsies.push({ jobId, bottleneck: autopsy.bottleneck, costDomains: autopsy.costDomains,
      topOperators: autopsy.topOperators, suggestions: autopsy.suggestions })
    const compared = await request(`/api/query/jobs/${jobId}/what-if`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ execute: true, workers: 8, combiner: true, cacheWarm: true })
    })
    if (!compared.measured.checksumMatch) throw new Error('Measured what-if changed a checksum')
    whatIf.push({ jobId, projection: compared.projection, measured: compared.measured })
  }
  const workload = await request('/api/workloads', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: `cost-lab-${Date.now()}`, description: 'Five canonical Cost Analyzer queries', jobIds: jobs })
  })
  const replay = await request(`/api/workloads/${workload.id}/replay`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ overrides: { cacheLevel: 'MEMORY_AND_DISK', cacheBudgetBytes: 67108864, combiner: true } })
  })
  if (!replay.results.every(item => item.checksumMatch)) throw new Error('Workload replay detected a checksum regression')
  console.log(JSON.stringify({
    status: 'passed', canonicalQueries: queries.length, datasetSnapshot: dataset.snapshotId,
    autopsies, whatIf,
    measuredSpeedups: whatIf.filter(item => item.measured.comparison.latencyMs < item.measured.baseline.latencyMs).length,
    workload: { id: workload.id, replayId: replay.replayId, checksumMatches: replay.results.filter(item => item.checksumMatch).length, results: replay.results }
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
