'use strict'

const WebSocket = require('ws')

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const WS_URL = API_URL.replace(/^http/, 'ws')
const MEASURED_RUNS = Number(process.env.RUNS || 5)

function generateSortedCsv (rows = Number(process.env.ROWS || 100000)) {
  const output = ['id,category,amount']
  for (let id = 1; id <= rows; id++) output.push(`${id},category_${id % 20},${id % 1000}`)
  return `${output.join('\n')}\n`
}

async function jsonRequest (url, options) {
  const response = await fetch(url, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

async function upload (csv) {
  const body = new FormData()
  body.append('file', new Blob([csv], { type: 'text/csv' }), 'storage_ablation.csv')
  return jsonRequest(`${API_URL}/api/datasets/upload`, { method: 'POST', body })
}

async function setFormat (datasetId, format) {
  return jsonRequest(`${API_URL}/api/datasets/${datasetId}/storage-format`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ format })
  })
}

async function execute (datasetId, sql) {
  const submitted = await jsonRequest(`${API_URL}/api/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ datasetId, sql })
  })
  await new Promise((resolve, reject) => {
    const ws = new WebSocket(`${WS_URL}/ws`)
    const timer = setTimeout(() => reject(new Error(`Timed out: ${submitted.jobId}`)), 60_000)
    ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', jobId: submitted.jobId })))
    ws.on('message', raw => {
      const event = JSON.parse(raw)
      if (event.type === 'error') reject(new Error(event.message))
      if (event.type === 'complete') resolve()
      if (event.type === 'error' || event.type === 'complete') {
        clearTimeout(timer)
        ws.close()
      }
    })
    ws.on('error', reject)
  })
  return jsonRequest(`${API_URL}/api/query/jobs/${submitted.jobId}`)
}

function percentile (values, p) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)]
}

async function measure (datasetId, format, sql) {
  await setFormat(datasetId, format)
  await execute(datasetId, sql)
  const runs = []
  for (let index = 0; index < MEASURED_RUNS; index++) {
    const result = await execute(datasetId, sql)
    runs.push({
      durationMs: result.job.execution_time_ms,
      scannedBytes: result.tasks.reduce((sum, task) => sum + Number(task.bytes_scanned || 0), 0),
      skippedBytes: result.tasks.reduce((sum, task) => sum + Number(task.bytes_skipped || 0), 0),
      transferredBytes: result.tasks.reduce((sum, task) => sum + Number(task.transferred_bytes || 0), 0),
      peakTaskMemoryBytes: Math.max(...result.tasks.map(task => Number(task.peak_memory_bytes || 0))),
      cpuTimeMicros: result.tasks.reduce((sum, task) => sum + Number(task.cpu_time_micros || 0), 0),
      rows: result.job.result_row_count
    })
  }
  return {
    format,
    repetitions: runs.length,
    p50Ms: percentile(runs.map(run => run.durationMs), 0.5),
    p95Ms: percentile(runs.map(run => run.durationMs), 0.95),
    medianScannedBytes: percentile(runs.map(run => run.scannedBytes), 0.5),
    medianSkippedBytes: percentile(runs.map(run => run.skippedBytes), 0.5),
    medianTransferredBytes: percentile(runs.map(run => run.transferredBytes), 0.5),
    medianPeakTaskMemoryBytes: percentile(runs.map(run => run.peakTaskMemoryBytes), 0.5),
    medianCpuTimeMicros: percentile(runs.map(run => run.cpuTimeMicros), 0.5),
    resultRows: runs[0].rows
  }
}

async function main () {
  const rowCount = Number(process.env.ROWS || 100000)
  const uploaded = await upload(generateSortedCsv(rowCount))
  const lowerBound = Math.floor(rowCount * 0.99)
  const sql = `SELECT id, amount FROM storage_ablation WHERE id > ${lowerBound} ORDER BY id ASC`
  const memorySql = 'SELECT SUM(amount) AS total, AVG(amount) AS average FROM storage_ablation'
  const csv = await measure(uploaded.datasetId, 'csv', sql)
  const csvMemory = await measure(uploaded.datasetId, 'csv', memorySql)
  const parquet = await measure(uploaded.datasetId, 'parquet', sql)
  const parquetMemory = await measure(uploaded.datasetId, 'parquet', memorySql)
  const rawSql = 'SELECT id, category, amount FROM storage_ablation WHERE id <= 10000 ORDER BY id ASC'
  const partialSql = 'SELECT category, COUNT(*) AS rows, SUM(amount) AS total FROM storage_ablation WHERE id <= 10000 GROUP BY category ORDER BY category ASC'
  const rawTransfer = await measure(uploaded.datasetId, 'parquet', rawSql)
  const partialTransfer = await measure(uploaded.datasetId, 'parquet', partialSql)
  if (csv.resultRows !== parquet.resultRows) throw new Error('Storage formats returned different row counts')
  if (parquet.medianScannedBytes >= csv.medianScannedBytes) {
    throw new Error(`Parquet did not reduce scanned bytes: CSV=${csv.medianScannedBytes}, Parquet=${parquet.medianScannedBytes}`)
  }
  if (parquetMemory.medianPeakTaskMemoryBytes >= csvMemory.medianPeakTaskMemoryBytes) {
    throw new Error(`Parquet did not reduce peak task memory: CSV=${csvMemory.medianPeakTaskMemoryBytes}, Parquet=${parquetMemory.medianPeakTaskMemoryBytes}`)
  }
  if (partialTransfer.medianTransferredBytes >= rawTransfer.medianTransferredBytes) {
    throw new Error(`Partial aggregation did not reduce transfer: raw=${rawTransfer.medianTransferredBytes}, partial=${partialTransfer.medianTransferredBytes}`)
  }
  console.log(JSON.stringify({
    status: 'passed', datasetId: uploaded.datasetId, rowCount, query: sql,
    csv, parquet,
    scannedByteReduction: 1 - parquet.medianScannedBytes / csv.medianScannedBytes,
    memoryWorkload: {
      query: memorySql,
      csv: csvMemory,
      parquet: parquetMemory,
      peakTaskMemoryReduction: 1 - parquetMemory.medianPeakTaskMemoryBytes / csvMemory.medianPeakTaskMemoryBytes
    },
    ablations: {
      storageAndVectorization: {
        legacyRowCsv: csvMemory,
        vectorizedParquet: parquetMemory,
        note: 'CSV uses the streamed row executor; Parquet uses vectorized DuckDB batches.'
      },
      pruning: {
        fullParquetBytes: parquet.medianScannedBytes + parquet.medianSkippedBytes,
        scannedBytes: parquet.medianScannedBytes,
        skippedBytes: parquet.medianSkippedBytes
      },
      partialAggregation: {
        rawProjection: rawTransfer,
        workerPartialAggregate: partialTransfer,
        transferReduction: 1 - partialTransfer.medianTransferredBytes / rawTransfer.medianTransferredBytes
      }
    }
  }, null, 2))
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exit(1)
})
