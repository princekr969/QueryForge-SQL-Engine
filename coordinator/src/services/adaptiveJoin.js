'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const db = require('../db')
const { executeTaskOnWorker } = require('../grpc/workerClient')
const sketchesPath = fs.existsSync('/shared/sketches.js') ? '/shared/sketches' : path.join(__dirname, '../../../shared/sketches')
const { sketchFromJSON } = require(sketchesPath)

function address (worker) { return `${worker.address}:${worker.port}` }

function chooseJoinStrategy (leftBytes, rightBytes, options = {}) {
  const localThreshold = Number(options.localThreshold || process.env.LOCAL_JOIN_MAX_BYTES || 2 * 1024 * 1024)
  const broadcastThreshold = Number(options.broadcastThreshold || process.env.BROADCAST_JOIN_MAX_BYTES || 16 * 1024 * 1024)
  const total = leftBytes + rightBytes
  const smaller = Math.min(leftBytes, rightBytes)
  const larger = Math.max(leftBytes, rightBytes, 1)
  if (total <= localThreshold) return { strategy: 'local', reason: 'both inputs fit the single-worker locality budget' }
  if (smaller <= broadcastThreshold && smaller / larger <= 0.35) {
    return { strategy: 'broadcast', reason: 'the smaller input is within the broadcast budget and at most 35% of the probe input' }
  }
  return { strategy: 'hash_shuffle', reason: 'neither input is sufficiently small to broadcast safely' }
}

function detectHotBuckets (probeStats, skewFactor = 1.5) {
  const average = probeStats.reduce((sum, item) => sum + Number(item.rows || 0), 0) /
    Math.max(1, probeStats.length)
  return probeStats.map((item, bucket) => ({ ...item, bucket }))
    .filter(item => Number(item.rows || 0) > average * skewFactor && item.sourceRows.filter(Boolean).length > 1)
}

async function runShuffleMap ({ jobId, dataset, table, partitions, plan, workers, buckets, side, bloom }) {
  const column = table.alias === plan.join.leftAlias ? plan.join.leftColumn : plan.join.rightColumn
  return Promise.all(partitions.map(async (partition, sourceIndex) => {
    let lastError
    for (let attempt = 1; attempt <= Math.min(3, workers.length); attempt++) {
      const worker = workers[(sourceIndex + attempt - 1) % workers.length]
      const taskId = crypto.randomUUID()
      await db.query(
        `INSERT INTO tasks (id, job_id, worker_id, partition_id, logical_partition_key, status, started_at, attempt_number)
         VALUES ($1,$2,$3,$4,$5,'running',NOW(),$6)`,
        [taskId, jobId, worker.workerId, partition.id, `shuffle-${side}-${sourceIndex}`, attempt]
      )
      try {
        worker.activeTasks = (worker.activeTasks || 0) + 1
        const results = await executeTaskOnWorker(address(worker), {
          task_id: taskId,
          job_id: jobId,
          partition_path: partition.parquet_path,
          storage_format: 'parquet',
          partition_row_count: Number(partition.row_count || 0),
          partition_byte_size: Number(partition.parquet_byte_size || 0),
          shuffle_spec_json: JSON.stringify({
            prefix: `shuffle/${jobId}`,
            side,
            datasetId: dataset.id,
            sourceIndex,
            column,
            buckets,
            bloom
          })
        }, 120000)
        const terminal = results.at(-1)
        if (terminal.shuffle_paths?.length !== buckets) throw new Error('Shuffle worker returned an incomplete bucket set')
        await db.query(
          `UPDATE tasks SET status='completed', completed_at=NOW(), rows_processed=$2::integer,
           rows_scanned=$2::bigint, bytes_scanned=$3, transferred_bytes=$4,
           cpu_time_micros=$5, operator_metrics_json=$6 WHERE id=$1`,
          [taskId, Number(terminal.rows_scanned || 0), Number(terminal.bytes_scanned || 0),
            Number(terminal.bytes_written || 0), Number(terminal.cpu_time_micros || 0),
            JSON.stringify({ operator: 'HashShuffleMap', side, buckets })]
        )
        const stats = terminal.sketch_state?.length
          ? JSON.parse(Buffer.from(terminal.sketch_state).toString('utf8'))
          : { bucketRows: Array(buckets).fill(0), bucketBytes: Array(buckets).fill(0) }
        return { paths: terminal.shuffle_paths, bytesWritten: Number(terminal.bytes_written || 0), ...stats }
      } catch (error) {
        lastError = error
        await db.query(
          `UPDATE tasks SET status='failed', completed_at=NOW(), error_message=$2 WHERE id=$1`,
          [taskId, error.message]
        )
      } finally {
        worker.activeTasks = Math.max(0, (worker.activeTasks || 1) - 1)
      }
    }
    throw lastError
  }))
}

async function buildBloom ({ jobId, partitions, column, workers }) {
  const states = await Promise.all(partitions.map(async (partition, index) => {
    const worker = workers[index % workers.length]
    const results = await executeTaskOnWorker(address(worker), {
      task_id: crypto.randomUUID(),
      job_id: jobId,
      partition_path: partition.parquet_path,
      storage_format: 'parquet',
      partition_row_count: Number(partition.row_count || 0),
      partition_byte_size: Number(partition.parquet_byte_size || 0),
      sketch_spec_json: JSON.stringify({ operation: 'bloom', column, bits: 262144, hashes: 5 })
    }, 120000)
    const terminal = results.at(-1)
    if (!terminal?.sketch_state?.length) throw new Error('Bloom build returned no state')
    return sketchFromJSON(JSON.parse(Buffer.from(terminal.sketch_state).toString('utf8')))
  }))
  const merged = states[0]
  for (const state of states.slice(1)) merged.merge(state)
  return merged.toJSON()
}

async function materializeHashShuffle ({ jobId, datasets, tables, partitionSets, plan, workers, buckets, skewFactor }) {
  const startedAt = Date.now()
  const inputBytes = partitionSets.map(partitions => partitions.reduce((sum, item) => sum + Number(item.parquet_byte_size || 0), 0))
  const buildIndex = inputBytes[0] <= inputBytes[1] ? 0 : 1
  const buildTable = tables[buildIndex]
  const buildColumn = buildTable.alias === plan.join.leftAlias ? plan.join.leftColumn : plan.join.rightColumn
  const bloom = await buildBloom({ jobId, partitions: partitionSets[buildIndex], column: buildColumn, workers })
  const left = await runShuffleMap({
    jobId, dataset: datasets[0], table: tables[0], partitions: partitionSets[0], plan, workers, buckets, side: 'left',
    bloom: buildIndex === 0 ? null : bloom
  })
  const right = await runShuffleMap({
    jobId, dataset: datasets[1], table: tables[1], partitions: partitionSets[1], plan, workers, buckets, side: 'right',
    bloom: buildIndex === 1 ? null : bloom
  })
  const bucketPaths = side => Array.from({ length: buckets }, (_, bucket) => side.map(source => source.paths[bucket]))
  const bucketTotals = side => Array.from({ length: buckets }, (_, bucket) => ({
    rows: side.reduce((sum, source) => sum + Number(source.bucketRows[bucket] || 0), 0),
    bytes: side.reduce((sum, source) => sum + Number(source.bucketBytes[bucket] || 0), 0),
    sourceRows: side.map(source => Number(source.bucketRows[bucket] || 0))
  }))
  const leftStats = bucketTotals(left)
  const rightStats = bucketTotals(right)
  const probeStats = buildIndex === 0 ? rightStats : leftStats
  const hotBuckets = detectHotBuckets(
    probeStats,
    Number(skewFactor || process.env.SKEW_SPLIT_FACTOR || 1.5)
  )
  return {
    leftBuckets: bucketPaths(left),
    rightBuckets: bucketPaths(right),
    bytesWritten: [...left, ...right].reduce((sum, item) => sum + item.bytesWritten, 0),
    bloomBytes: Buffer.byteLength(JSON.stringify(bloom)),
    leftStats,
    rightStats,
    hotBuckets,
    executionTimeMs: Date.now() - startedAt
  }
}

module.exports = { materializeHashShuffle, chooseJoinStrategy, detectHotBuckets }
