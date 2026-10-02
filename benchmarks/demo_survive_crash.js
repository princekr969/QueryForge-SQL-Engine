'use strict'

const { execFileSync } = require('node:child_process')
const path = require('node:path')
const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROOT = path.resolve(__dirname, '..')

async function request (route, options) {
  const response = await fetch(`${API_URL}${route}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function fixture () {
  const lines = ['id,bucket,amount']
  for (let id = 1; id <= 80000; id++) lines.push(`${id},b${id % 32},${(id * 7919) % 1000}`)
  const body = new FormData()
  body.append('partitionCount', '8')
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), 'stage_survival.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function execute (datasetId, crash) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId, sql: 'SELECT bucket, COUNT(*) AS rows, SUM(amount) AS total FROM stage_survival GROUP BY bucket ORDER BY bucket ASC',
      resourceBudget: { workerLimit: 2, maxExecutionMs: 30000 }
    })
  })
  if (crash) {
    const deadline = Date.now() + 10000
    let observed = false
    while (Date.now() < deadline) {
      try {
        const current = await request(`/api/query/jobs/${submitted.jobId}`)
        observed = current.tasks.some(task => task.worker_id === 'worker-2' && task.status === 'running')
      } catch {}
      if (observed) break
      await new Promise(resolve => setTimeout(resolve, 50))
    }
    if (!observed) throw new Error('worker-2 never received the synchronized slow partition')
    execFileSync('docker', ['compose', 'stop', '-t', '0', 'worker-2'], { cwd: ROOT, stdio: 'ignore' })
  }
  await new Promise((resolve, reject) => {
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error('Demo query timed out')), 40000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'complete') resolve()
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timeout); socket.close() }
    })
    socket.on('error', reject)
  })
  return request(`/api/query/jobs/${submitted.jobId}`)
}

async function main () {
  const startedAt = Date.now()
  execFileSync('docker', ['compose', 'start', 'worker-2'], { cwd: ROOT, stdio: 'ignore' })
  const dataset = await fixture()
  const baseline = await execute(dataset.datasetId, false)
  await request('/api/chaos', {
    method: 'PUT', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delay', targetWorker: 'worker-2', partitionIndex: 1, delayMs: 5000 })
  })
  const recovered = await execute(dataset.datasetId, true)
  if (baseline.job.result_checksum !== recovered.job.result_checksum) throw new Error('Crash changed the checksum')
  console.log(JSON.stringify({
    status: 'passed', stageMoment: 'worker-2 killed during a delayed partition',
    checksumBefore: baseline.job.result_checksum, checksumAfter: recovered.job.result_checksum,
    failedAttempts: recovered.tasks.filter(task => task.status === 'failed').length,
    speculativeAttempts: recovered.tasks.filter(task => Number(task.attempt_number) > 1).length,
    elapsedMs: Date.now() - startedAt
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exitCode = 1 }).finally(() => {
  try { execFileSync('docker', ['compose', 'start', 'worker-2'], { cwd: ROOT, stdio: 'ignore' }) } catch {}
})
