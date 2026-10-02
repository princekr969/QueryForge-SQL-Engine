'use strict'

const { execFileSync } = require('node:child_process')
const path = require('node:path')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROOT = path.resolve(__dirname, '..')

async function request (pathName, options) {
  const response = await fetch(`${API_URL}${pathName}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function waitFor (check, timeoutMs = 40000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const value = await check()
      if (value) return value
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 500))
  }
  throw new Error('Timed out waiting for recovered job')
}

async function main () {
  const lines = ['id,value']
  for (let id = 1; id <= 150000; id++) lines.push(`${id},${id % 1000}`)
  const body = new FormData()
  body.append('file', new Blob([`${lines.join('\n')}\n`], { type: 'text/csv' }), 'restart.csv')
  const dataset = await request('/api/datasets/upload', { method: 'POST', body })
  await request('/api/chaos', {
    method: 'PUT', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delay', delayMs: 10000, occurrences: 16 })
  })
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      datasetId: dataset.datasetId,
      sql: 'SELECT SUM(value) AS total, AVG(value) AS average FROM restart',
      resourceBudget: { maxExecutionMs: 60000, maxResultRows: 10 }
    })
  })
  await waitFor(async () => {
    const current = await request(`/api/query/jobs/${submitted.jobId}`)
    return current.job.status === 'running' && current.tasks.some(task => task.status === 'running')
  })
  execFileSync('docker', ['compose', 'kill', '-s', 'SIGKILL', 'coordinator'], { cwd: ROOT, stdio: 'pipe' })
  execFileSync('docker', ['compose', 'start', 'coordinator'], { cwd: ROOT, stdio: 'pipe' })
  await waitFor(() => request('/api/health'))
  const recovered = await waitFor(async () => {
    const status = await request(`/api/query/jobs/${submitted.jobId}`)
    if (status.job.status === 'failed') throw new Error('Recovered job failed')
    return status.job.status === 'completed' ? status : null
  }, 60000)
  const abandoned = recovered.tasks.filter(task =>
    task.error_message === 'Coordinator restarted before attempt commit'
  ).length
  if (abandoned === 0) throw new Error('No interrupted attempts were durably abandoned')
  if (!recovered.job.result_checksum) throw new Error('Recovered job has no checksum')
  console.log(JSON.stringify({
    status: 'passed', jobId: submitted.jobId, checksum: recovered.job.result_checksum,
    abandonedAttempts: abandoned, totalAttempts: recovered.tasks.length
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
