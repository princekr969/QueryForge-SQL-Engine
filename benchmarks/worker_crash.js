'use strict'

const { execFileSync } = require('node:child_process')
const path = require('node:path')
const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROOT = path.resolve(__dirname, '..')

async function request (pathName, options) {
  const response = await fetch(`${API_URL}${pathName}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function execute (datasetId, crash = false) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId,
      sql: 'SELECT bucket, COUNT(*) AS rows, SUM(amount) AS total FROM chaos GROUP BY bucket ORDER BY bucket ASC',
      resourceBudget: { workerLimit: 2, maxExecutionMs: 5000, maxResultRows: 1000 }
    })
  })
  if (crash) {
    const deadline = Date.now() + 10000
    let observed = false
    while (Date.now() < deadline) {
      try {
        const current = await request(`/api/query/jobs/${submitted.jobId}`)
        const injectedTaskIds = new Set(current.chaosEvents
          .filter(event => event.mode === 'delay')
          .map(event => event.task_id))
        observed = current.tasks.some(task =>
          task.worker_id === 'worker-1' && task.status === 'running' && injectedTaskIds.has(task.id)
        )
      } catch {}
      if (observed) break
      await new Promise(resolve => setTimeout(resolve, 50))
    }
    if (!observed) throw new Error('worker-1 never received a running attempt before the crash')
    execFileSync('docker', ['compose', 'stop', '-t', '0', 'worker-1'], { cwd: ROOT, stdio: 'pipe' })
  }
  await new Promise((resolve, reject) => {
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error(`Timed out ${submitted.jobId}`)), 70000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'complete') resolve()
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timeout); socket.close() }
    })
    socket.on('error', reject)
  })
  if (crash) await new Promise(resolve => setTimeout(resolve, 3500))
  return request(`/api/query/jobs/${submitted.jobId}`)
}

async function main () {
  execFileSync('docker', ['compose', 'start', 'worker-1'], { cwd: ROOT, stdio: 'pipe' })
  const workerDeadline = Date.now() + 15000
  let workerReady = false
  while (Date.now() < workerDeadline) {
    const workers = await request('/api/workers')
    workerReady = workers.some(worker => worker.id === 'worker-1' && worker.liveStatus === 'active')
    if (workerReady) break
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  if (!workerReady) throw new Error('worker-1 did not become active')
  const datasets = await request('/api/datasets')
  const dataset = datasets.find(item => item.name === 'chaos')
  if (!dataset) throw new Error('Run the chaos-matrix benchmark first to create the seeded chaos dataset')
  const baseline = await execute(dataset.id)
  await request('/api/chaos', {
    method: 'PUT', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delay', targetWorker: 'worker-1', delayMs: 1000 })
  })
  const recovered = await execute(dataset.id, true)
  const failed = recovered.tasks.filter(task => task.status === 'failed')
  if (recovered.job.result_checksum !== baseline.job.result_checksum) throw new Error('Worker crash changed result checksum')
  if (!failed.some(task => task.worker_id === 'worker-1')) throw new Error('No worker-1 attempt failed during the container crash')
  console.log(JSON.stringify({
    status: 'passed',
    crashedWorker: 'worker-1',
    baselineChecksum: baseline.job.result_checksum,
    recoveredChecksum: recovered.job.result_checksum,
    failedAttempts: failed.length,
    totalAttempts: recovered.tasks.length
  }, null, 2))
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exitCode = 1
}).finally(async () => {
  try { await request('/api/chaos', { method: 'DELETE' }) } catch {}
  try { execFileSync('docker', ['compose', 'start', 'worker-1'], { cwd: ROOT, stdio: 'pipe' }) } catch {}
})
