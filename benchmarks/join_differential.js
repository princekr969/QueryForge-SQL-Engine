'use strict'

const { DuckDBInstance } = require('@duckdb/node-api')
const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const WS_URL = API_URL.replace(/^http/, 'ws')

function datasets () {
  const customers = ['id,region,tier']
  for (let id = 1; id <= 50; id++) customers.push(`${id},${['east', 'north', 'south', 'west'][id % 4]},${id % 5}`)
  const orders = ['id,customer_id,amount']
  for (let id = 1; id <= 900; id++) orders.push(`${id},${1 + (id % 50)},${(id * 17) % 1000}`)
  return { customers: `${customers.join('\n')}\n`, orders: `${orders.join('\n')}\n` }
}

async function upload (name, csv) {
  const body = new FormData()
  body.append('file', new Blob([csv], { type: 'text/csv' }), `${name}.csv`)
  const response = await fetch(`${API_URL}/api/datasets/upload`, { method: 'POST', body })
  const payload = await response.json()
  if (!response.ok) throw new Error(`Upload failed: ${JSON.stringify(payload)}`)
  return payload.datasetId
}

async function execute (ordersId, customersId, sql) {
  const response = await fetch(`${API_URL}/api/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ datasetId: ordersId, datasetIds: { customers: customersId }, sql })
  })
  const submitted = await response.json()
  if (!response.ok) throw new Error(`Query rejected: ${JSON.stringify(submitted)}`)
  return new Promise((resolve, reject) => {
    const rows = []
    const ws = new WebSocket(`${WS_URL}/ws`)
    const timer = setTimeout(() => reject(new Error(`Timed out: ${submitted.jobId}`)), 30_000)
    ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    ws.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'row') rows.push(event.data)
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete') resolve({ rows, jobId: submitted.jobId })
      if (event.type === 'error' || event.type === 'complete') {
        clearTimeout(timer)
        ws.close()
      }
    })
    ws.on('error', reject)
  })
}

function normalize (rows) {
  return rows.map(row => Object.fromEntries(Object.entries(row).map(([key, value]) => {
    if (value === null || value === '') return [key, null]
    if (/^-?(?:\d+\.?\d*|\d*\.\d+)$/.test(String(value))) return [key, Number(value)]
    return [key, String(value)]
  })))
}

async function main () {
  const csv = datasets()
  const [ordersId, customersId] = await Promise.all([
    upload('orders', csv.orders), upload('customers', csv.customers)
  ])
  const instance = await DuckDBInstance.create(':memory:')
  const connection = await instance.connect()
  await connection.run('CREATE TABLE orders(id BIGINT, customer_id BIGINT, amount DOUBLE)')
  await connection.run('CREATE TABLE customers(id BIGINT, region VARCHAR, tier BIGINT)')
  const orderAppender = await connection.createAppender('orders')
  for (const line of csv.orders.trim().split('\n').slice(1)) {
    const [id, customerId, amount] = line.split(',').map(Number)
    orderAppender.appendBigInt(BigInt(id)); orderAppender.appendBigInt(BigInt(customerId)); orderAppender.appendDouble(amount); orderAppender.endRow()
  }
  orderAppender.closeSync()
  const customerAppender = await connection.createAppender('customers')
  for (const line of csv.customers.trim().split('\n').slice(1)) {
    const [id, region, tier] = line.split(',')
    customerAppender.appendBigInt(BigInt(id)); customerAppender.appendVarchar(region); customerAppender.appendBigInt(BigInt(tier)); customerAppender.endRow()
  }
  customerAppender.closeSync()

  const queries = [
    'SELECT o.id AS order_id, c.region AS region FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE o.amount >= 950 ORDER BY order_id ASC LIMIT 20',
    'SELECT c.region AS region, COUNT(*) AS orders, SUM(o.amount) AS total, AVG(o.amount) AS average FROM orders o INNER JOIN customers c ON o.customer_id = c.id GROUP BY c.region ORDER BY region ASC',
    'SELECT COUNT(*) AS orders, MIN(o.amount) AS minimum, MAX(o.amount) AS maximum FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.tier >= 2'
  ]
  const strategies = new Set()
  for (const sql of queries) {
    const [execution, expectedReader] = await Promise.all([
      execute(ordersId, customersId, sql), connection.runAndReadAll(sql)
    ])
    const actualRows = normalize(execution.rows)
    const expectedRows = normalize(expectedReader.getRowObjectsJson())
    if (JSON.stringify(actualRows) !== JSON.stringify(expectedRows)) {
      throw new Error(`Join mismatch for ${sql}\nexpected=${JSON.stringify(expectedRows)}\nactual=${JSON.stringify(actualRows)}`)
    }
    const jobResponse = await fetch(`${API_URL}/api/query/jobs/${execution.jobId}`)
    const job = await jobResponse.json()
    strategies.add(job.job.plan_json.joinStrategy)
  }
  connection.closeSync()
  console.log(JSON.stringify({ status: 'passed', strategies: [...strategies], queries: queries.length, ordersId, customersId }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
