'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const db = require('../db')
const { workerRegistry } = require('./workerRegistry')
const { executeTaskOnWorker } = require('../grpc/workerClient')

const sharedSketches = fs.existsSync('/shared/sketches.js')
  ? '/shared/sketches'
  : path.join(__dirname, '../../../shared/sketches')
const { sketchFromJSON } = require(sharedSketches)

const OPERATIONS = new Set(['approx_count_distinct', 'approx_percentile', 'heavy_hitters', 'sample'])

function integerOption (value, fallback, minimum, maximum, name) {
  const parsed = value === undefined ? fallback : Number(value)
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`)
  }
  return parsed
}

function buildSpec (input) {
  if (!OPERATIONS.has(input.operation)) throw new Error(`operation must be one of: ${[...OPERATIONS].join(', ')}`)
  if (typeof input.column !== 'string' || input.column.length === 0) throw new Error('column is required')

  if (input.operation === 'approx_count_distinct') {
    return { operation: input.operation, column: input.column, precision: integerOption(input.precision, 12, 4, 16, 'precision') }
  }
  if (input.operation === 'approx_percentile') {
    const quantile = input.quantile === undefined ? 0.5 : Number(input.quantile)
    if (!Number.isFinite(quantile) || quantile < 0 || quantile > 1) throw new Error('quantile must be from 0 to 1')
    return { operation: input.operation, column: input.column, quantile, capacity: integerOption(input.capacity, 200, 20, 5000, 'capacity') }
  }
  if (input.operation === 'heavy_hitters') {
    return {
      operation: input.operation,
      column: input.column,
      width: integerOption(input.width, 2048, 64, 65536, 'width'),
      depth: integerOption(input.depth, 5, 2, 10, 'depth'),
      k: integerOption(input.k, 10, 1, 100, 'k')
    }
  }
  return { operation: input.operation, column: input.column, size: integerOption(input.sampleSize, 100, 1, 10000, 'sampleSize') }
}

function configuredError (spec) {
  if (spec.operation === 'approx_count_distinct') return 1.04 / Math.sqrt(2 ** spec.precision)
  if (spec.operation === 'approx_percentile') return 2 / Math.sqrt(spec.capacity)
  if (spec.operation === 'heavy_hitters') return Math.E / spec.width
  return null
}

function resultFor (sketch, spec) {
  if (spec.operation === 'approx_count_distinct') return { estimate: sketch.estimate() }
  if (spec.operation === 'approx_percentile') return { estimate: sketch.quantile(spec.quantile), quantile: spec.quantile }
  if (spec.operation === 'heavy_hitters') return { items: sketch.topK(spec.k) }
  return { items: sketch.values() }
}

function observedError (result, exactValue, operation) {
  if (exactValue === undefined || exactValue === null || result.estimate === null || result.estimate === undefined) return null
  const exact = Number(exactValue)
  if (!Number.isFinite(exact)) throw new Error('exactValue must be numeric')
  const absolute = Math.abs(Number(result.estimate) - exact)
  return operation === 'approx_count_distinct' ? absolute / Math.max(1, Math.abs(exact)) : absolute
}

async function runApproximateQuery (input) {
  const startedAt = Date.now()
  if (typeof input.datasetId !== 'string' || input.datasetId.length === 0) throw new Error('datasetId is required')
  const spec = buildSpec(input)

  const datasetResult = await db.query('SELECT * FROM datasets WHERE id = $1', [input.datasetId])
  if (datasetResult.rowCount === 0) throw new Error('Dataset not found')
  const dataset = datasetResult.rows[0]
  const schema = typeof dataset.schema_json === 'string' ? JSON.parse(dataset.schema_json) : dataset.schema_json
  if (!(schema?.columns || []).some(column => column.name === spec.column)) throw new Error(`Unknown column: ${spec.column}`)
  if (dataset.storage_format !== 'parquet' || !dataset.columnar_committed_at) {
    throw new Error('Approximate analytics require a committed Parquet snapshot')
  }

  const partitionResult = await db.query(
    'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index', [input.datasetId]
  )
  if (partitionResult.rowCount === 0 || partitionResult.rows.some(partition => !partition.parquet_path)) {
    throw new Error('Dataset has no complete Parquet partitions')
  }
  const workers = [...workerRegistry.values()].filter(worker =>
    worker.status === 'active' && worker.capabilities?.includes('sketch')
  )
  if (workers.length === 0) throw new Error('No active sketch-capable workers available')

  const states = await Promise.all(partitionResult.rows.map(async (partition, index) => {
    let lastError
    for (let attempt = 0; attempt < Math.min(3, workers.length); attempt++) {
      const worker = workers[(index + attempt) % workers.length]
      const taskId = crypto.randomUUID()
      worker.activeTasks = (worker.activeTasks || 0) + 1
      try {
        const results = await executeTaskOnWorker(`${worker.address}:${worker.port}`, {
          task_id: taskId,
          job_id: '',
          partition_path: partition.parquet_path,
          storage_format: 'parquet',
          partition_row_count: Number(partition.row_count || 0),
          partition_byte_size: Number(partition.parquet_byte_size || 0),
          partition_stats_json: partition.stats_json ? JSON.stringify(partition.stats_json) : '',
          sketch_spec_json: JSON.stringify(spec)
        }, integerOption(input.maxExecutionMs, 30000, 1000, 300000, 'maxExecutionMs'))
        const terminal = results.at(-1)
        if (!terminal?.sketch_state?.length) throw new Error(`Worker ${worker.workerId} returned no sketch state`)
        return {
          sketch: sketchFromJSON(JSON.parse(Buffer.from(terminal.sketch_state).toString('utf8'))),
          stateBytes: terminal.sketch_state.length,
          transferredBytes: terminal.sketch_state.length,
          rowsScanned: Number(terminal.rows_scanned || 0)
        }
      } catch (error) {
        lastError = error
      } finally {
        worker.activeTasks = Math.max(0, (worker.activeTasks || 1) - 1)
      }
    }
    throw lastError
  }))

  const merged = states[0].sketch
  for (const state of states.slice(1)) merged.merge(state.sketch)
  const result = resultFor(merged, spec)
  const errorBound = configuredError(spec)
  const measuredError = observedError(result, input.exactValue, spec.operation)
  const stateBytes = Buffer.byteLength(JSON.stringify(merged.toJSON()))
  const transferredBytes = states.reduce((sum, item) => sum + item.transferredBytes, 0)
  const rowsScanned = states.reduce((sum, item) => sum + item.rowsScanned, 0)
  const executionTimeMs = Date.now() - startedAt

  const saved = await db.query(
    `INSERT INTO approximate_runs
       (dataset_id, column_name, operation, config_json, result_json, state_bytes,
        transferred_bytes, rows_scanned, execution_time_ms, configured_error, observed_error)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id, created_at`,
    [input.datasetId, spec.column, spec.operation, JSON.stringify(spec), JSON.stringify(result),
      stateBytes, transferredBytes, rowsScanned, executionTimeMs, errorBound, measuredError]
  )

  return {
    runId: saved.rows[0].id,
    datasetId: input.datasetId,
    operation: spec.operation,
    column: spec.column,
    config: spec,
    result,
    configuredError: errorBound,
    observedError: measuredError,
    stateBytes,
    transferredBytes,
    rowsScanned,
    partitions: states.length,
    executionTimeMs,
    createdAt: saved.rows[0].created_at
  }
}

module.exports = { runApproximateQuery, buildSpec, configuredError }
