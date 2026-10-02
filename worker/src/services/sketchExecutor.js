'use strict'

const fs = require('fs/promises')
const syncFs = require('fs')
const path = require('path')
const { DuckDBInstance } = require('@duckdb/node-api')
const sharedSketches = syncFs.existsSync('/shared/sketches.js')
  ? '/shared/sketches'
  : path.join(__dirname, '../../../shared/sketches')
const { HyperLogLog, KllSketch, FrequencySketch, PriorityReservoir, BloomFilter } = require(sharedSketches)

function quoteIdentifier (value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

function quoteLiteral (value) {
  return `'${String(value).replace(/'/g, "''")}'`
}

function createSketch (spec) {
  if (spec.operation === 'approx_count_distinct') return new HyperLogLog(spec.precision)
  if (spec.operation === 'approx_percentile') return new KllSketch(spec.capacity)
  if (spec.operation === 'heavy_hitters') return new FrequencySketch(spec.width, spec.depth, spec.k)
  if (spec.operation === 'sample') return new PriorityReservoir(spec.size)
  if (spec.operation === 'bloom') return new BloomFilter(spec.bits, spec.hashes)
  throw new Error(`Unknown sketch operation: ${spec.operation}`)
}

async function executeSketchTask (localPath, request, onResult) {
  const spec = JSON.parse(request.sketch_spec_json)
  const spillDirectory = `/tmp/queryforge-sketch-${request.task_id}`
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: process.env.WORKER_MEMORY_LIMIT || '256MB',
    temp_directory: spillDirectory,
    threads: process.env.WORKER_THREADS || '2'
  })
  const connection = await instance.connect()
  let rows = 0
  try {
    const source = request.storage_format === 'parquet'
      ? `read_parquet(${quoteLiteral(localPath)})`
      : `read_csv_auto(${quoteLiteral(localPath)}, header = true, nullstr = '')`
    if (spec.operation === 'partition_histogram') {
      const histogram = Array.from({ length: spec.buckets }, () => 0)
      const result = await connection.stream(
        `SELECT CAST(hash(${quoteIdentifier(spec.column)}) % ${Number(spec.buckets)} AS INTEGER) AS "__bucket", COUNT(*) AS "__count" FROM ${source} GROUP BY 1`
      )
      for await (const batch of result.yieldRowObjectJson()) {
        for (const row of batch) {
          histogram[Number(row.__bucket)] += Number(row.__count)
          rows += Number(row.__count)
        }
      }
      const state = Buffer.from(JSON.stringify({ operation: spec.operation, buckets: spec.buckets, counts: histogram }))
      onResult({
        task_id: request.task_id, is_aggregated: false, column_names: [], rows: [], groups: [],
        arrow_ipc: Buffer.alloc(0), sketch_state: state, is_complete: true, rows_scanned: rows,
        bytes_scanned: Number(request.partition_byte_size || 0), bytes_skipped: 0
      })
      return
    }
    const sketch = createSketch(spec)
    const result = await connection.stream(
      `SELECT ${quoteIdentifier(spec.column)} AS "__value" FROM ${source}`
    )
    for await (const batch of result.yieldRowObjectJson()) {
      for (const row of batch) {
        const value = row.__value
        if (spec.operation === 'sample') sketch.add(value, `${request.partition_path}:${rows}`)
        else sketch.add(value)
        rows++
      }
    }
    const state = Buffer.from(JSON.stringify(sketch.toJSON()))
    onResult({
      task_id: request.task_id,
      is_aggregated: false,
      column_names: [], rows: [], groups: [], arrow_ipc: Buffer.alloc(0),
      sketch_state: state,
      is_complete: true,
      rows_scanned: rows,
      bytes_scanned: Number(request.partition_byte_size || 0),
      bytes_skipped: 0
    })
  } finally {
    connection.closeSync()
    await fs.rm(spillDirectory, { recursive: true, force: true })
  }
}

module.exports = { executeSketchTask, createSketch }
