'use strict'

const { execFileSync } = require('node:child_process')
const crypto = require('node:crypto')
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

async function waitFor (fn, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs
  let last
  while (Date.now() < deadline) {
    try { last = await fn(); if (last) return last } catch {}
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  throw new Error(`Condition timed out; last=${JSON.stringify(last)}`)
}

async function execute (datasetId, sql) {
  const submitted = await request('/api/query', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ datasetId, sql })
  })
  const results = await new Promise((resolve, reject) => {
    const rows = []
    const socket = new WebSocket(`${API_URL.replace(/^http/, 'ws')}/ws`)
    const timeout = setTimeout(() => reject(new Error('Materialized reference query timed out')), 30000)
    socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    socket.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'row') rows.push(event.data)
      if (event.type === 'complete') resolve(rows)
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete' || event.type === 'error') { clearTimeout(timeout); socket.close() }
    })
    socket.on('error', reject)
  })
  return { job: await request(`/api/query/jobs/${submitted.jobId}`), results }
}

function events (base, start, count) {
  return Array.from({ length: count }, (_, offset) => {
    const index = start + offset
    return { ts: new Date(base + Math.floor(index / 50) * 1000).toISOString(), user_id: `u-${index}`, revenue: 1 + (index % 23), url: ['/home', '/search', '/checkout'][index % 3] }
  })
}

function compose (args) {
  return execFileSync('docker', ['compose', ...args], {
    cwd: ROOT, stdio: 'pipe',
    env: { ...process.env, COORDINATOR_HTTP_PORT: new URL(API_URL).port || '3000', COORDINATOR_GRPC_PORT: process.env.COORDINATOR_GRPC_PORT || '15050' }
  })
}

async function validateAdditionalWindows (suffix, base) {
  const hopTopic = `hop_${suffix}`
  const hop = await request('/api/streams/register', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: hopTopic, topic: hopTopic, eventTimeColumn: 'ts', windowType: 'HOP',
      sizeMs: 3000, slideMs: 1000, allowedLatenessMs: 10000, sumColumn: 'revenue',
      workerDelayMs: 1200
    })
  })
  const hopEvents = Array.from({ length: 6 }, (_, index) => ({
    ts: new Date(base + index * 1000).toISOString(), revenue: index + 1
  }))
  await request(`/api/streams/publish/${hopTopic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ events: hopEvents })
  })
  await new Promise(resolve => setTimeout(resolve, 150))
  compose(['kill', '-s', 'SIGKILL', 'worker-1'])
  compose(['up', '-d', 'worker-1'])
  const hopState = await waitFor(async () => {
    const state = await request(`/api/streams/${hop.id}`)
    return state.batches.reduce((sum, batch) => sum + Number(batch.event_count), 0) === 6 ? state : null
  })
  const hopMemberships = hopState.windows.reduce((sum, window) => sum + Number(window.event_count), 0)
  if (hopMemberships !== 18) throw new Error(`HOP membership mismatch: expected 18, received ${hopMemberships}`)
  if (!hopState.batches[0]?.worker_id || hopState.batches[0].worker_id === 'worker-1') {
    throw new Error(`Stream worker failover was not observed: ${hopState.batches[0]?.worker_id || 'missing attribution'}`)
  }

  const sessionTopic = `session_${suffix}`
  const session = await request('/api/streams/register', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: sessionTopic, topic: sessionTopic, eventTimeColumn: 'ts', windowType: 'SESSION',
      sizeMs: 1000, gapMs: 1000, allowedLatenessMs: 10000,
      groupByColumn: 'user_id', sumColumn: 'revenue'
    })
  })
  await request(`/api/streams/publish/${sessionTopic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ events: [
      { ts: new Date(base).toISOString(), user_id: 'student-a', revenue: 2 },
      { ts: new Date(base + 500).toISOString(), user_id: 'student-a', revenue: 3 },
      { ts: new Date(base + 2000).toISOString(), user_id: 'student-a', revenue: 7 },
      { ts: new Date(base + 3000).toISOString(), user_id: 'student-a', revenue: 5 }
    ] })
  })
  const sessionState = await waitFor(async () => {
    const state = await request(`/api/streams/${session.id}`)
    return state.batches.reduce((sum, batch) => sum + Number(batch.event_count), 0) === 4 ? state : null
  })
  const sessionCounts = sessionState.windows.map(window => Number(window.event_count)).sort((a, b) => a - b)
  const sessionRevenue = sessionState.windows.reduce((sum, window) => sum + Number(window.result.revenue), 0)
  if (sessionCounts.join(',') !== '2,2' || sessionRevenue !== 17) {
    throw new Error(`SESSION mismatch: counts=${sessionCounts.join(',')} revenue=${sessionRevenue}`)
  }
  return {
    HOP: { inputEvents: 6, windowMemberships: hopMemberships, windows: hopState.windows.length, workerFailure: `worker-1 SIGKILL; committed by ${hopState.batches[0].worker_id}` },
    SESSION: { inputEvents: 4, sessions: sessionState.windows.length, sessionCounts, exactRevenue: sessionRevenue }
  }
}

async function main () {
  const suffix = crypto.randomBytes(4).toString('hex')
  const topic = `exactly_once_${suffix}`
  const registered = await request('/api/streams/register', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: topic, topic, eventTimeColumn: 'ts', windowType: 'TUMBLE', sizeMs: 1000,
      allowedLatenessMs: 2000, distinctColumn: 'user_id', sumColumn: 'revenue',
      heavyHitterColumn: 'url', hllPrecision: 14, trackExactEvaluation: true
    })
  })
  if (!(registered.configuredHllError < 0.01)) throw new Error('HLL configured error is not below 1%')
  const base = Date.now()
  const first = events(base, 0, 200)
  await request(`/api/streams/publish/${topic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ events: first })
  })
  await waitFor(async () => {
    const state = await request(`/api/streams/${registered.id}`)
    return state.windows.reduce((sum, window) => sum + Number(window.event_count), 0) === 200 ? state : null
  })

  const second = events(base, 200, 400)
  await request(`/api/streams/publish/${topic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ events: second })
  })
  compose(['kill', '-s', 'SIGKILL', 'coordinator'])
  compose(['up', '-d', 'coordinator'])
  await waitFor(() => request('/api/health'))
  const recovered = await waitFor(async () => {
    const state = await request(`/api/streams/${registered.id}`)
    return state.windows.reduce((sum, window) => sum + Number(window.event_count), 0) === 600 ? state : null
  }, 90000)
  const windowEvents = recovered.windows.reduce((sum, window) => sum + Number(window.event_count), 0)
  const revenue = recovered.windows.reduce((sum, window) => sum + Number(window.result.revenue), 0)
  const acceptedLateEvent = { ts: new Date(base + 10500).toISOString(), user_id: 'accepted-late-user', revenue: 7, url: '/accepted-late' }
  const expectedRevenue = [...first, ...second].reduce((sum, event) => sum + event.revenue, 0)
  if (windowEvents !== 600 || revenue !== expectedRevenue) throw new Error(`Committed state mismatch events=${windowEvents}, revenue=${revenue}`)

  await request(`/api/streams/publish/${topic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ events: [acceptedLateEvent] })
  })
  const acceptedLate = await waitFor(async () => {
    const state = await request(`/api/streams/${registered.id}`)
    const count = state.windows.reduce((sum, window) => sum + Number(window.event_count), 0)
    return state.acceptedLateEvents === 1 && count === 601 ? state : null
  })
  const acceptedRevenue = expectedRevenue + acceptedLateEvent.revenue

  await request(`/api/streams/publish/${topic}`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ events: [{ ts: new Date(base - 10000).toISOString(), user_id: 'late-user', revenue: 999, url: '/late' }] })
  })
  const late = await waitFor(async () => {
    const state = await request(`/api/streams/${registered.id}`)
    return state.lateEvents === 1 ? state : null
  })
  if (late.windows.reduce((sum, window) => sum + Number(window.event_count), 0) !== 601) throw new Error('Audited late event mutated committed windows')
  const materialized = await request(`/api/streams/${registered.id}/materialize`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: `${topic}_snapshot`, partitionCount: 3 })
  })
  const materializedReference = await execute(
    materialized.dataset.datasetId,
    `SELECT COUNT(*) AS windows, SUM(revenue) AS revenue FROM ${topic}_snapshot`
  )
  const referenceRow = materializedReference.results[0]
  if (Number(referenceRow.windows) !== late.windows.length || Number(referenceRow.revenue) !== acceptedRevenue) {
    throw new Error(`Materialized epoch mismatch: ${JSON.stringify(referenceRow)}`)
  }
  const maxObservedHllError = Math.max(...late.windows.map(window => Number(window.result.distinctObservedError || 0)))
  const maxObservedCmsError = Math.max(...late.windows.map(window => Number(window.result.frequencyObservedError || 0)))
  const additionalWindows = await validateAdditionalWindows(suffix, base + 60000)
  console.log(JSON.stringify({
    status: 'passed', queryId: registered.id, topic,
    committedInputEvents: 602, acceptedWindowEvents: 601, acceptedLateEvents: acceptedLate.acceptedLateEvents, lateEvents: late.lateEvents,
    exactRevenue: acceptedRevenue, expectedRevenue: acceptedRevenue, duplicateOrLostEvents: late.windows.reduce((sum, window) => sum + Number(window.event_count), 0) - 601,
    committedBatches: late.batches.length, durableOffsets: late.offsets,
    hllConfiguredError: registered.configuredHllError,
    hllObservedMaxError: maxObservedHllError,
    cmsConfiguredError: late.windows[0]?.result.frequencyConfiguredError,
    cmsObservedMaxError: maxObservedCmsError,
    windows: {
      TUMBLE: { inputEvents: 601, acceptedWindowEvents: 601 },
      ...additionalWindows
    },
    materializedDatasetId: materialized.dataset.datasetId,
    materializedOutputEpoch: materialized.outputEpoch,
    materializedReference: referenceRow,
    crash: 'coordinator SIGKILL after Kafka publish; durable state/offset replay converged exactly'
  }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
