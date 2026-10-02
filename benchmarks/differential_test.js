'use strict'

const { DuckDBInstance } = require('@duckdb/node-api')
const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const WS_URL = API_URL.replace(/^http/, 'ws')

function generateCsv (rowCount = 300, seed = 0x51f15e) {
  let state = seed >>> 0
  const random = () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state / 0x100000000
  }
  const regions = ['east', 'north', 'south', 'west']
  const lines = ['id,region,amount,units']
  for (let id = 1; id <= rowCount; id++) {
    const region = regions[Math.floor(random() * regions.length)]
    const amount = id % 17 === 0 ? '' : Math.floor(random() * 1000)
    const units = 1 + Math.floor(random() * 20)
    lines.push(`${id},${region},${amount},${units}`)
  }
  return `${lines.join('\n')}\n`
}

function buildQueries () {
  const queries = []
  for (let i = 0; i < 25; i++) {
    queries.push(`SELECT id, region, amount FROM differential WHERE amount >= ${i * 31} ORDER BY id ASC LIMIT ${5 + (i % 11)}`)
    queries.push(`SELECT id, region FROM differential WHERE region = '${['east', 'north', 'south', 'west'][i % 4]}' AND id > ${i * 7} ORDER BY id DESC LIMIT ${4 + (i % 9)}`)
    queries.push(`SELECT COUNT(*) AS rows, COUNT(amount) AS counted, SUM(amount) AS total, AVG(amount) AS average, MIN(amount) AS minimum, MAX(amount) AS maximum FROM differential WHERE id >= ${i * 9 + 1}`)
    queries.push(`SELECT region, COUNT(*) AS rows, SUM(amount) AS total, AVG(amount) AS average FROM differential WHERE amount >= ${i * 23} GROUP BY region ORDER BY region ASC`)
  }
  return queries
}

async function uploadDataset (csv) {
  const body = new FormData()
  body.append('file', new Blob([csv], { type: 'text/csv' }), 'differential.csv')
  const response = await fetch(`${API_URL}/api/datasets/upload`, { method: 'POST', body })
  const payload = await response.json()
  if (!response.ok) throw new Error(`Upload failed (${response.status}): ${JSON.stringify(payload)}`)
  return payload.datasetId
}

async function runQueryForge (datasetId, sql) {
  const response = await fetch(`${API_URL}/api/query`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ datasetId, sql })
  })
  const payload = await response.json()
  if (!response.ok) throw new Error(`Query rejected (${response.status}): ${payload.error}`)

  return new Promise((resolve, reject) => {
    const rows = []
    const ws = new WebSocket(`${WS_URL}/ws`)
    const timer = setTimeout(() => {
      ws.terminate()
      reject(new Error(`Timed out waiting for job ${payload.jobId}`))
    }, 20_000)

    ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', jobId: payload.jobId })))
    ws.on('message', raw => {
      const event = JSON.parse(raw.toString())
      if (event.type === 'row') rows.push(event.data)
      if (event.type === 'error') {
        clearTimeout(timer)
        ws.close()
        reject(new Error(event.message))
      }
      if (event.type === 'complete') {
        clearTimeout(timer)
        ws.close()
        resolve(rows)
      }
    })
    ws.on('error', reject)
  })
}

function normalizeValue (value) {
  if (value === null || value === undefined || value === '') return null
  const numeric = typeof value === 'number' || typeof value === 'bigint' ||
    (typeof value === 'string' && /^-?(?:\d+\.?\d*|\d*\.\d+)(?:e[+-]?\d+)?$/i.test(value))
  if (numeric) return Math.round(Number(value) * 1e9) / 1e9
  return String(value)
}

function canonicalize (rows) {
  return rows.map(row => Object.fromEntries(
    Object.keys(row).sort().map(key => [key, normalizeValue(row[key])])
  ))
}

async function main () {
  const csv = generateCsv()
  const datasetId = await uploadDataset(csv)
  const instance = await DuckDBInstance.create(':memory:')
  const connection = await instance.connect()
  // DuckDB and QueryForge consume identical bytes. DuckDB's CSV reader accepts
  // the same generated rows through its typed appender.
  await connection.run('CREATE TABLE differential(id BIGINT, region VARCHAR, amount DOUBLE, units BIGINT)')
  const rows = csv.trim().split('\n').slice(1).map(line => line.split(','))
  const appender = await connection.createAppender('differential')
  for (const [id, region, amount, units] of rows) {
    appender.appendBigInt(BigInt(id))
    appender.appendVarchar(region)
    if (amount === '') appender.appendNull()
    else appender.appendDouble(Number(amount))
    appender.appendBigInt(BigInt(units))
    appender.endRow()
  }
  appender.closeSync()

  const queries = buildQueries()
  const startedAt = Date.now()
  for (let index = 0; index < queries.length; index++) {
    const sql = queries[index]
    const [actual, reader] = await Promise.all([
      runQueryForge(datasetId, sql),
      connection.runAndReadAll(sql)
    ])
    const expected = reader.getRowObjectsJson()
    const actualCanonical = canonicalize(actual)
    const expectedCanonical = canonicalize(expected)
    if (JSON.stringify(actualCanonical) !== JSON.stringify(expectedCanonical)) {
      throw new Error(`Differential mismatch at query ${index + 1}: ${sql}\nexpected=${JSON.stringify(expectedCanonical)}\nactual=${JSON.stringify(actualCanonical)}`)
    }
  }

  connection.closeSync()
  console.log(JSON.stringify({
    status: 'passed',
    reference: 'DuckDB',
    seed: '0x51f15e',
    datasetRows: rows.length,
    queries: queries.length,
    durationMs: Date.now() - startedAt,
    datasetId
  }, null, 2))
}

main().catch(err => {
  console.error(err.stack || err.message)
  process.exit(1)
})
