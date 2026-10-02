'use strict'

const crypto = require('crypto')
const db = require('../db')
const { workerRegistry } = require('./workerRegistry')
const { executeTaskOnWorker } = require('../grpc/workerClient')

async function explorePartitioning ({ datasetId, column, buckets = 8, maxExecutionMs = 30000 }) {
  if (!datasetId || typeof datasetId !== 'string') throw new Error('datasetId is required')
  if (!column || typeof column !== 'string') throw new Error('column is required')
  buckets = Number(buckets)
  if (!Number.isInteger(buckets) || buckets < 2 || buckets > 32) throw new Error('buckets must be an integer from 2 to 32')
  maxExecutionMs = Number(maxExecutionMs)
  if (!Number.isFinite(maxExecutionMs) || maxExecutionMs < 1000 || maxExecutionMs > 300000) throw new Error('maxExecutionMs must be from 1000 to 300000')

  const datasetResult = await db.query('SELECT * FROM datasets WHERE id = $1', [datasetId])
  if (datasetResult.rowCount === 0) throw new Error('Dataset not found')
  const dataset = datasetResult.rows[0]
  const schema = typeof dataset.schema_json === 'string' ? JSON.parse(dataset.schema_json) : dataset.schema_json
  if (!(schema?.columns || []).some(item => item.name === column)) throw new Error(`Unknown column: ${column}`)
  if (dataset.storage_format !== 'parquet' || !dataset.columnar_committed_at) throw new Error('Partition explorer requires a committed Parquet snapshot')

  const partitions = (await db.query('SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index', [datasetId])).rows
  const workers = [...workerRegistry.values()].filter(worker => worker.status === 'active' && worker.capabilities?.includes('sketch'))
  if (workers.length === 0) throw new Error('No active sketch-capable workers available')
  const partials = await Promise.all(partitions.map(async (partition, index) => {
    const worker = workers[index % workers.length]
    const results = await executeTaskOnWorker(`${worker.address}:${worker.port}`, {
      task_id: crypto.randomUUID(), job_id: '', partition_path: partition.parquet_path,
      storage_format: 'parquet', partition_row_count: Number(partition.row_count || 0),
      partition_byte_size: Number(partition.parquet_byte_size || 0),
      sketch_spec_json: JSON.stringify({ operation: 'partition_histogram', column, buckets })
    }, maxExecutionMs)
    const counts = JSON.parse(Buffer.from(results.at(-1).sketch_state).toString('utf8')).counts
    const sourceBytes = Number(partition.parquet_byte_size || 0)
    const sourceRows = counts.reduce((sum, value) => sum + Number(value || 0), 0)
    const remainderBucket = Math.max(0, counts.findLastIndex(value => Number(value || 0) > 0))
    let assignedBytes = 0
    const estimatedBytes = counts.map((count, bucket) => {
      const value = bucket === remainderBucket
        ? sourceBytes - assignedBytes
        : Math.floor(sourceBytes * Number(count || 0) / Math.max(1, sourceRows))
      assignedBytes += value
      return value
    })
    return { counts, estimatedBytes }
  }))
  const counts = Array.from({ length: buckets }, (_, bucket) => partials.reduce((sum, item) => sum + Number(item.counts[bucket] || 0), 0))
  const byteCounts = Array.from({ length: buckets }, (_, bucket) => partials.reduce((sum, item) => sum + Number(item.estimatedBytes[bucket] || 0), 0))
  const total = counts.reduce((sum, count) => sum + count, 0)
  const mean = total / buckets
  const deviation = Math.sqrt(counts.reduce((sum, count) => sum + ((count - mean) ** 2), 0) / buckets)
  return {
    datasetId, column, function: `duckdb_hash(${column}) mod ${buckets}`, buckets, totalRows: total,
    totalInputBytes: byteCounts.reduce((sum, value) => sum + value, 0),
    byteMethod: 'catalog Parquet bytes allocated in proportion to exact per-partition bucket rows',
    coefficientOfVariation: mean > 0 ? deviation / mean : 0,
    skewRatio: mean > 0 ? Math.max(...counts) / mean : 0,
    hotBuckets: counts.map((rows, bucket) => ({ bucket, rows, estimatedBytes: byteCounts[bucket], share: total ? rows / total : 0 }))
      .filter(item => item.rows > mean * 1.5),
    distribution: counts.map((rows, bucket) => ({ bucket, rows, estimatedBytes: byteCounts[bucket], share: total ? rows / total : 0 }))
  }
}

module.exports = { explorePartitioning }
