'use strict'

const WebSocket = require('ws')
const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'

async function request (path, options) {
  const response = await fetch(`${API_URL}${path}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function upload () {
  const lines = ['id,bucket,amount']
  for (let id = 1; id <= 100000; id++) lines.push(`${id},${id % 100},${id % 1000}`)
  const body = new FormData()
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), 'chaos.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function execute (datasetId) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId,
      sql: 'SELECT bucket, COUNT(*) AS rows, SUM(amount) AS total FROM chaos GROUP BY bucket ORDER BY bucket ASC',
      resourceBudget: { maxExecutionMs: 60000, maxResultRows: 1000 }
    })
  })
  await new Promise((resolve, reject) => {
    const ws = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timer = setTimeout(() => reject(new Error(`Timed out ${submitted.jobId}`)), 70000)
    ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    ws.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'complete') resolve()
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timer); ws.close() }
    })
    ws.on('error', reject)
  })
  return request(`/api/query/jobs/${submitted.jobId}`)
}

async function main () {
  const dataset = await upload()
  const baseline = await execute(dataset.datasetId)
  const matrix = []
  for (const rule of [
    { mode: 'network_loss' },
    { mode: 'corrupted_input' },
    { mode: 'delay', delayMs: 700 },
    { mode: 'duplicate', delayMs: 700 },
    { mode: 'skew', partitionIndex: 1, delayMs: 700 }
  ]) {
    await request('/api/chaos', {
      method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(rule)
    })
    const result = await execute(dataset.datasetId)
    if (result.job.result_checksum !== baseline.job.result_checksum) {
      throw new Error(`${rule.mode} changed result checksum`)
    }
    if (!result.chaosEvents.some(event => event.mode === rule.mode)) {
      throw new Error(`${rule.mode} was not injected`)
    }
    matrix.push({
      mode: rule.mode,
      status: result.job.status,
      checksumPreserved: true,
      attempts: result.tasks.length,
      failedAttempts: result.tasks.filter(task => task.status === 'failed').length,
      speculativeAttempts: result.tasks.filter(task => Number(task.attempt_number) === 2).length
    })
  }
  console.log(JSON.stringify({ status: 'passed', baselineChecksum: baseline.job.result_checksum, matrix }, null, 2))
}

main().catch(async error => {
  try { await request('/api/chaos', { method: 'DELETE' }) } catch {}
  console.error(error.stack || error.message)
  process.exit(1)
})
