'use strict'

/**
 * jobManager.js
 * Creates and tracks Jobs and Tasks in PostgreSQL.
 * Dispatches tasks to workers via gRPC and drives the full query lifecycle.
 */

const { v4: uuidv4 }         = require('uuid')
const crypto                  = require('crypto')
const { tableFromIPC }         = require('apache-arrow')
const db                      = require('../db')
const { executeTaskOnWorker } = require('../grpc/workerClient')
const { workerRegistry }      = require('./workerRegistry')
const { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas } = require('./queryPlanner')
const { mergeResults }        = require('./resultMerger')
const { getJobSubscribers }   = require('../websocket/wsServer')
const chaosController          = require('./chaosController')
const { materializeHashShuffle, chooseJoinStrategy } = require('./adaptiveJoin')
const { speculationThresholdMs } = require('./speculationPolicy')

// OTel
const { metrics, trace } = require('@opentelemetry/api')
const meter  = metrics.getMeter('coordinator')
const tracer = trace.getTracer('coordinator')

const queryDurationHistogram = meter.createHistogram('dataforge_query_duration_ms', {
  description: 'End-to-end query execution time in milliseconds'
})
const tasksCounter = meter.createCounter('dataforge_tasks_total', {
  description: 'Total tasks created, by status'
})
const activeWorkersGauge = meter.createObservableGauge('dataforge_active_workers', {
  description: 'Number of currently active workers'
})
// addBatchObservableCallback is the correct API for observable gauges in OTel JS SDK
meter.addBatchObservableCallback((observableResult) => {
  let count = 0
  for (const [, w] of workerRegistry.entries()) {
    if (w.status === 'active') count++
  }
  observableResult.observe(activeWorkersGauge, count)
}, [activeWorkersGauge])

const MAX_PARTITION_ATTEMPTS = 3
const SPECULATION_DELAY_MS = Number(process.env.SPECULATION_DELAY_MS || 250)

// ── Helpers ───────────────────────────────────────────────────────────────────

function getActiveWorkers () {
  const active = []
  for (const [, w] of workerRegistry.entries()) {
    if (w.status === 'active') active.push(w)
  }
  return active
}

function getEligibleWorkers (plan, workerLimit = 0) {
  const eligible = getActiveWorkers()
    .filter(worker => !plan.join || (worker.capabilities?.includes('join') &&
      (plan.joinStrategy === 'broadcast' || plan.joinStrategy === 'local' || worker.capabilities?.includes('shuffle'))))
    .sort((left, right) => left.workerId.localeCompare(right.workerId))
  return workerLimit > 0 ? eligible.slice(0, workerLimit) : eligible
}

function pushToSubscribers (jobId, event) {
  const subscribers = getJobSubscribers(jobId)
  const payload     = JSON.stringify(event)
  for (const ws of subscribers) {
    if (ws.queryForgeReplayPending === true) {
      ws.queryForgePendingEvents = ws.queryForgePendingEvents || []
      ws.queryForgePendingEvents.push(payload)
    } else if (ws.readyState === 1 /* OPEN */) {
      ws.send(payload)
    }
  }
}

function abortableDelay (milliseconds, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new Error('Task attempt cancelled'))
    const timer = setTimeout(resolve, milliseconds)
    signal?.addEventListener('abort', () => {
      clearTimeout(timer)
      reject(new Error('Task attempt cancelled'))
    }, { once: true })
  })
}

// ── Main entry point ──────────────────────────────────────────────────────────

/**
 * Execute a full distributed query.
 *
 * @param {string} sql
 * @param {string} datasetId
 * @param {string} [preGeneratedJobId] - pre-generated so route can return it before execution starts
 * @returns {Promise<{ jobId: string }>}
 */
async function executeQuery (sql, datasetId, preGeneratedJobId, datasetIds = {}, resourceBudget = {}) {
  const jobId   = preGeneratedJobId || uuidv4()
  const startMs = Date.now()

  const span = tracer.startSpan('job.execute', {
    attributes: {
      'job.id':     jobId,
      'dataset.id': datasetId,
      'worker.count': 3
    }
  })

  try {
    // ── 1. Parse SQL → execution plan ───────────────────────────────────────────
    let plan
    try {
      plan = buildExecutionPlan(sql)
    } catch (err) {
      throw new Error(`Query plan error: ${err.message}`)
    }
    const queryFingerprint = crypto.createHash('sha256')
      .update(`${sql.trim().replace(/\s+/g, ' ')}\0${datasetId}\0${JSON.stringify(datasetIds)}\0${resourceBudget.joinStrategy || 'auto'}`)
      .digest('hex')
    const feedbackResult = await db.query('SELECT * FROM query_feedback WHERE fingerprint = $1', [queryFingerprint])
    const runtimeFeedback = feedbackResult.rows[0] || null
    if (runtimeFeedback) {
      plan.runtimeFeedback = {
        executions: runtimeFeedback.executions,
        emaResultRows: runtimeFeedback.ema_result_rows,
        emaStragglerRatio: runtimeFeedback.ema_straggler_ratio,
        recommendedShuffleBuckets: runtimeFeedback.recommended_shuffle_buckets,
        hotBucketCount: runtimeFeedback.hot_bucket_count
      }
    }

    // ── 2. Look up dataset + partitions ─────────────────────────────────────────
    const dsResult = await db.query('SELECT * FROM datasets WHERE id = $1', [datasetId])
    if (dsResult.rows.length === 0) throw new Error(`Dataset ${datasetId} not found`)
    const dataset = dsResult.rows[0]
    if (String(plan.tableName).toLowerCase() !== String(dataset.name).toLowerCase()) {
      throw new Error(`FROM table "${plan.tableName}" does not match selected dataset "${dataset.name}"`)
    }
    let probeDataset = dataset
    let buildDataset = null
    let probeTable = plan.tables?.[0]
    let buildTable = null
    let joinDatasets = null
    let joinPartitionSets = null
    if (plan.join) {
      const joinedDatasets = [dataset]
      for (const table of plan.tables.slice(1)) {
        const joinedId = datasetIds[table.name]
        if (!joinedId) throw new Error(`datasetIds.${table.name} is required for the joined table`)
        const joinedResult = await db.query('SELECT * FROM datasets WHERE id = $1', [joinedId])
        if (joinedResult.rowCount === 0 || String(joinedResult.rows[0].name).toLowerCase() !== table.name.toLowerCase()) {
          throw new Error(`Dataset binding failed for joined table ${table.name}`)
        }
        joinedDatasets.push(joinedResult.rows[0])
      }
      validateJoinPlanAgainstSchemas(plan, joinedDatasets)
      joinDatasets = joinedDatasets
      if (joinedDatasets.some(item => item.storage_format !== 'parquet')) {
        throw new Error('Distributed joins require committed Parquet snapshots')
      }
      const bytes = async (id) => Number((await db.query(
        'SELECT COALESCE(SUM(parquet_byte_size), 0)::bigint AS bytes FROM partitions WHERE dataset_id = $1', [id]
      )).rows[0].bytes)
      const leftBytes = await bytes(joinedDatasets[0].id)
      const rightBytes = await bytes(joinedDatasets[1].id)
      const buildIndex = leftBytes <= rightBytes ? 0 : 1
      const probeIndex = 1 - buildIndex
      buildDataset = joinedDatasets[buildIndex]
      probeDataset = joinedDatasets[probeIndex]
      buildTable = plan.tables[buildIndex]
      probeTable = plan.tables[probeIndex]
      const adaptive = chooseJoinStrategy(leftBytes, rightBytes)
      const strategyOverride = resourceBudget.joinStrategy && resourceBudget.joinStrategy !== 'auto'
        ? resourceBudget.joinStrategy
        : null
      plan.joinStrategy = strategyOverride || adaptive.strategy
      plan.joinStrategyReason = strategyOverride
        ? `explicit experiment override (${strategyOverride})`
        : adaptive.reason
      plan.estimatedBuildBytes = buildIndex === 0 ? leftBytes : rightBytes
      plan.estimatedProbeBytes = probeIndex === 0 ? leftBytes : rightBytes
    } else {
      validatePlanAgainstSchema(plan, dataset.schema_json)
    }
    const combinerEnabled = plan.aggregations.length > 0 && resourceBudget.combiner !== false
    plan.mapreduce = {
      combiner: {
        operator: 'LocalCombiner',
        enabled: combinerEnabled,
        mode: combinerEnabled ? 'merge-safe partial aggregation' : 'controlled row-state ablation',
        states: Object.fromEntries(plan.aggregations.map(aggregation => [
          aggregation.alias,
          aggregation.function === 'AVG' ? ['sum', 'count'] : [aggregation.function.toLowerCase(), 'count']
        ]))
      },
      operators: [
        { id: 'partition_map', operator: plan.join ? 'JoinMap' : 'PartitionMap' },
        ...(plan.aggregations.length ? [{ id: 'local_combiner', operator: 'LocalCombiner', enabled: combinerEnabled }] : []),
        { id: 'coordinator_reduce', operator: plan.aggregations.length ? 'FinalReduce' : 'ResultMerge' }
      ]
    }

    const partResult = await db.query(
      'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index',
      [probeDataset.id]
    )
    let partitions = partResult.rows
    if (partitions.length === 0) throw new Error(`Dataset ${datasetId} has no readable partitions`)
    let buildPartitions = plan.join
      ? (await db.query(
          'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index', [buildDataset.id]
        )).rows
      : []
    if (plan.join) {
      joinPartitionSets = await Promise.all(joinDatasets.map(item => db.query(
        'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index', [item.id]
      ).then(result => result.rows)))
    }

    // ── 3. Create job record in DB ───────────────────────────────────────────────
    const queryContext = JSON.stringify({ datasetIds })
    if (resourceBudget.resumeExisting) {
      const recovery = await db.getClient()
      try {
        await recovery.query('BEGIN')
        await recovery.query(
          `UPDATE tasks SET status = 'failed', completed_at = NOW(),
           error_message = 'Coordinator restarted before attempt commit'
           WHERE job_id = $1 AND status = 'running'`, [jobId]
        )
        await recovery.query('DELETE FROM job_result_rows WHERE job_id = $1', [jobId])
        await recovery.query(
          `UPDATE jobs SET status = 'running', completed_at = NULL, plan_json = $2,
           resource_budget_json = $3, query_context_json = $4 WHERE id = $1`,
          [jobId, JSON.stringify(plan), JSON.stringify(resourceBudget), queryContext]
        )
        await recovery.query('COMMIT')
      } catch (error) {
        await recovery.query('ROLLBACK')
        throw error
      } finally {
        recovery.release()
      }
    } else {
      await db.query(
        `INSERT INTO jobs
           (id, sql_query, dataset_id, status, plan_json, priority, resource_budget_json, query_context_json)
         VALUES ($1, $2, $3, 'running', $4, $5, $6, $7)`,
        [jobId, sql, datasetId, JSON.stringify(plan), resourceBudget.priority || 0,
          JSON.stringify(resourceBudget), queryContext]
      )
    }

    const lineageJobNode = await db.query(
      `INSERT INTO lineage_nodes
         (kind, operator, job_id, logical_partition_key, metadata_json, status)
       VALUES ('transformation', 'SQL_QUERY', $1, '__job__', $2, 'running')
       ON CONFLICT (job_id, logical_partition_key) DO UPDATE
         SET metadata_json = EXCLUDED.metadata_json, status = 'running'
       RETURNING id`,
      [jobId, JSON.stringify({ sql, plan })]
    )
    const inputDatasetIds = [datasetId, ...Object.values(datasetIds)]
    await db.query(
      `INSERT INTO lineage_edges (parent_id, child_id, edge_type)
       SELECT id, $2, 'input' FROM lineage_nodes
       WHERE kind = 'source' AND dataset_id = ANY($1::uuid[])
       ON CONFLICT DO NOTHING`,
      [inputDatasetIds, lineageJobNode.rows[0].id]
    )

    let activeWorkers = getEligibleWorkers(plan, resourceBudget.workerLimit)
    if (activeWorkers.length === 0) {
      await db.query(`UPDATE jobs SET status = 'failed', completed_at = NOW() WHERE id = $1`, [jobId])
      throw new Error('No active workers available')
    }

    if (plan.joinStrategy === 'local') {
      const primaryPaths = partitions.map(item => item.parquet_path)
      partitions = [{
        ...partitions[0],
        logicalPartitionKey: 'local-join',
        row_count: partitions.reduce((sum, item) => sum + Number(item.row_count || 0), 0),
        parquet_byte_size: partitions.reduce((sum, item) => sum + Number(item.parquet_byte_size || 0), 0),
        shufflePrimaryPaths: primaryPaths,
        shuffleSecondaryPaths: buildPartitions.map(item => item.parquet_path)
      }]
    } else if (plan.joinStrategy === 'hash_shuffle') {
      const feedbackBuckets = Number(runtimeFeedback?.recommended_shuffle_buckets || 0)
      const buckets = Math.max(1, Math.min(feedbackBuckets || activeWorkers.length, activeWorkers.length, 8))
      const shuffle = await materializeHashShuffle({
        jobId,
        datasets: joinDatasets,
        tables: plan.tables,
        partitionSets: joinPartitionSets,
        plan,
        workers: activeWorkers.filter(worker => worker.capabilities?.includes('shuffle')),
        buckets,
        skewFactor: resourceBudget.skewSplitFactor
      })
      const probeIndex = joinDatasets.findIndex(item => item.id === probeDataset.id)
      const primaryBuckets = probeIndex === 0 ? shuffle.leftBuckets : shuffle.rightBuckets
      const secondaryBuckets = probeIndex === 0 ? shuffle.rightBuckets : shuffle.leftBuckets
      const probeStats = probeIndex === 0 ? shuffle.leftStats : shuffle.rightStats
      const buildStats = probeIndex === 0 ? shuffle.rightStats : shuffle.leftStats
      const populatedPaths = (paths, stats) => paths.filter((_path, sourceIndex) => stats.sourceRows[sourceIndex] > 0)
      const hot = new Map(shuffle.hotBuckets.map(item => [item.bucket, item]))
      partitions = []
      for (let bucket = 0; bucket < buckets; bucket++) {
        // An inner join bucket with either side empty is provably empty and
        // should not create an invalid read_parquet([]) task.
        if (probeStats[bucket].rows === 0 || buildStats[bucket].rows === 0) continue
        const base = joinPartitionSets[probeIndex][bucket % joinPartitionSets[probeIndex].length]
        const hotBucket = hot.get(bucket)
        if (hotBucket) {
          primaryBuckets[bucket].forEach((sourcePath, sourceIndex) => {
            if (hotBucket.sourceRows[sourceIndex] === 0) return
            partitions.push({
              ...base,
              logicalPartitionKey: `bucket-${bucket}-source-${sourceIndex}`,
              parquet_path: sourcePath,
              row_count: hotBucket.sourceRows[sourceIndex],
              parquet_byte_size: Math.max(1, Math.ceil(hotBucket.bytes / primaryBuckets[bucket].length)),
              shufflePrimaryPaths: [sourcePath],
              shuffleSecondaryPaths: populatedPaths(secondaryBuckets[bucket], buildStats[bucket]),
              hotSplit: true
            })
          })
        } else {
          partitions.push({
            ...base,
            logicalPartitionKey: `bucket-${bucket}`,
            parquet_path: primaryBuckets[bucket][0],
            row_count: probeStats[bucket].rows,
            parquet_byte_size: probeStats[bucket].bytes,
            shufflePrimaryPaths: populatedPaths(primaryBuckets[bucket], probeStats[bucket]),
            shuffleSecondaryPaths: populatedPaths(secondaryBuckets[bucket], buildStats[bucket])
          })
        }
      }
      plan.shuffle = {
        buckets,
        logicalPartitions: partitions.length,
        hotBuckets: shuffle.hotBuckets.map(item => ({ bucket: item.bucket, rows: item.rows, splits: item.sourceRows.filter(Boolean).length })),
        bytesWritten: shuffle.bytesWritten,
        bloomBytes: shuffle.bloomBytes,
        executionTimeMs: shuffle.executionTimeMs
      }
      await db.query('UPDATE jobs SET plan_json = $2 WHERE id = $1', [jobId, JSON.stringify(plan)])
    }

    if (resourceBudget.recoverLogicalPartition) {
      partitions = partitions.filter(partition =>
        (partition.logicalPartitionKey || `partition-${partition.id}`) === resourceBudget.recoverLogicalPartition
      )
      if (partitions.length !== 1) throw new Error(`Recoverable partition not found: ${resourceBudget.recoverLogicalPartition}`)
      plan.recovery = { logicalPartitionKey: resourceBudget.recoverLogicalPartition, scope: 'single-partition' }
      await db.query('UPDATE jobs SET plan_json = $2 WHERE id = $1', [jobId, JSON.stringify(plan)])
    }

    pushToSubscribers(jobId, {
      type: 'progress',
      completedTasks: 0,
      totalTasks: partitions.length
    })

    // ── 4. Dispatch partitions in parallel; retry attempts remain owned by this
    //       promise so a detached result can never enter the final reduction. ────
    const workerAddress = (w) => `${w.address}:${w.port}`
    let completedPartitions = 0
    const completedAttemptDurations = []

    const executePartition = async (partition, partitionIndex) => {
      const attemptedWorkers = new Set()
      let lastError = null

      const chooseWorker = (attempt) => {
        const available = getEligibleWorkers(plan, resourceBudget.workerLimit)
        if (available.length === 0) throw new Error('No active workers available during retry')
        const untried = available.filter(worker => !attemptedWorkers.has(worker.workerId))
        const candidates = untried.length > 0 ? untried : available
        const minimumLoad = Math.min(...candidates.map(candidate => candidate.activeTasks || 0))
        const leastLoaded = candidates.filter(candidate => (candidate.activeTasks || 0) === minimumLoad)
        const worker = leastLoaded[(partitionIndex + attempt - 1) % leastLoaded.length]
        attemptedWorkers.add(worker.workerId)
        return { worker, minimumLoad }
      }

      const runAttempt = async (attempt, worker, minimumLoad, speculative = false, signal = null) => {
        const taskId = uuidv4()
        const attemptStartedAt = Date.now()

        await db.query(
          `INSERT INTO tasks
             (id, job_id, worker_id, partition_id, logical_partition_key, status, started_at, attempt_number)
           VALUES ($1, $2, $3, $4, $5, 'running', NOW(), $6)`,
          [taskId, jobId, worker.workerId, partition.id,
            partition.logicalPartitionKey || `partition-${partition.id}`, attempt]
        )
        tasksCounter.add(1, { status: 'running' })

        const taskRequest = {
          task_id:            taskId,
          job_id:             jobId,
          partition_path:     probeDataset.storage_format === 'parquet' ? partition.parquet_path : partition.minio_path,
          storage_format:     probeDataset.storage_format,
          partition_row_count: partition.row_count,
          partition_byte_size: probeDataset.storage_format === 'parquet'
            ? partition.parquet_byte_size
            : partition.csv_byte_size,
          partition_stats_json: partition.stats_json ? JSON.stringify(partition.stats_json) : '',
          predicates:         plan.predicates,
          select_columns:     plan.selectColumns,
          group_by_columns:   plan.groupByColumns,
          aggregations:       plan.aggregations,
          order_by_column:    plan.orderByColumn,
          order_by_direction: plan.orderByDirection,
          limit:              plan.limit,
          disable_combiner:   resourceBudget.combiner === false,
          cache_level:        resourceBudget.cacheLevel || 'NONE',
          cache_budget_bytes: Number(resourceBudget.cacheBudgetBytes || 256 * 1024 * 1024)
        }
        if (plan.join) {
          taskRequest.predicates = []
          taskRequest.primary_partition_paths = partition.shufflePrimaryPaths || []
          taskRequest.secondary_partition_paths = partition.shuffleSecondaryPaths || buildPartitions.map(item => item.parquet_path)
          taskRequest.primary_table_alias = probeTable.alias
          taskRequest.secondary_table_alias = buildTable.alias
          taskRequest.execution_sql = resourceBudget.combiner === false ? plan.workerSqlUncombined : plan.workerSql
          taskRequest.join_strategy = plan.joinStrategy
        }

        try {
          worker.activeTasks = (worker.activeTasks || 0) + 1
          const chaos = chaosController.consume({ workerId: worker.workerId, partitionIndex })
          if (chaos) {
            await db.query(
              `INSERT INTO chaos_events (job_id, task_id, mode, configuration_json)
               VALUES ($1, $2, $3, $4)`,
              [jobId, taskId, chaos.mode, JSON.stringify(chaos)]
            )
            pushToSubscribers(jobId, { type: 'chaos', taskId, mode: chaos.mode, workerId: worker.workerId })
            if (chaos.mode === 'delay' || chaos.mode === 'skew' || chaos.mode === 'duplicate') {
              await abortableDelay(chaos.delayMs || SPECULATION_DELAY_MS * 2, signal)
            } else if (chaos.mode === 'network_loss') {
              throw new Error('Injected network loss')
            } else if (chaos.mode === 'corrupted_input') {
              taskRequest.partition_path = `${taskRequest.partition_path}.corrupted`
            }
          }
          const partialResults = await executeTaskOnWorker(
            workerAddress(worker), taskRequest, Number(resourceBudget.maxExecutionMs || 30000), signal
          )
          const rowsReturned = partialResults.reduce(
            (sum, pr) => sum + (pr.rows?.length || pr.groups?.length ||
              (pr.arrow_ipc?.length ? tableFromIPC(pr.arrow_ipc).numRows : 0)), 0
          )
          const rowsPassedFilter = partialResults.reduce((sum, result) => {
            if (result.is_aggregated) {
              return sum + (result.groups || []).reduce((groupSum, group) => groupSum + Number(group.count || 0), 0)
            }
            return sum + (result.rows?.length || (result.arrow_ipc?.length ? tableFromIPC(result.arrow_ipc).numRows : 0))
          }, 0)
          const rowsScanned = Number(partialResults[0]?.rows_scanned || rowsReturned)
          const bytesScanned = Number(partialResults[0]?.bytes_scanned || 0)
          const bytesSkipped = Number(partialResults[0]?.bytes_skipped || 0)
          const peakMemoryBytes = Math.max(...partialResults.map(result => Number(result.peak_memory_bytes || 0)))
          const cpuTimeMicros = Math.max(...partialResults.map(result => Number(result.cpu_time_micros || 0)))
          const transferredBytes = partialResults.reduce((sum, result) =>
            sum + Number(result.arrow_ipc?.length || Buffer.byteLength(JSON.stringify(
              result.groups?.length ? result.groups : (result.rows || [])
            ))), 0)
          const partialResultChecksum = crypto.createHash('sha256').update(JSON.stringify(partialResults.map(result => ({
            isAggregated: result.is_aggregated,
            columnNames: result.column_names,
            rows: result.rows,
            groups: result.groups,
            arrowIpc: result.arrow_ipc ? Buffer.from(result.arrow_ipc).toString('base64') : '',
            isComplete: result.is_complete
          })))).digest('hex')
          const operatorMetrics = {
            strategy: plan.joinStrategy || 'partition_scan',
            estimatedRows: Number(partition.row_count || 0),
            actualRows: rowsReturned,
            selectivity: Number(partition.row_count || 0) > 0 ? rowsReturned / Number(partition.row_count) : 0,
            workerLoadAtDispatch: minimumLoad
          }

          await db.query(
            `UPDATE tasks SET status = 'completed', completed_at = NOW(),
             rows_processed = $2, rows_scanned = $3, bytes_scanned = $4,
             bytes_skipped = $5, peak_memory_bytes = $6,
             cpu_time_micros = $7, transferred_bytes = $8, operator_metrics_json = $9,
             partial_result_checksum = $10, is_winner = FALSE WHERE id = $1`,
            [taskId, rowsReturned, rowsScanned, bytesScanned, bytesSkipped, peakMemoryBytes, cpuTimeMicros,
              transferredBytes, JSON.stringify(operatorMetrics), partialResultChecksum]
          )
          await db.query(
            `INSERT INTO cache_events
               (job_id, task_id, worker_id, cache_level, cache_hit, object_path, bytes)
             VALUES ($1,$2,$3,$4,$5,$6,$7)`,
            [jobId, taskId, worker.workerId, partialResults[0]?.cache_level || 'NONE',
              partialResults[0]?.cache_hit || false, taskRequest.partition_path, bytesScanned]
          )
          tasksCounter.add(1, { status: 'completed' })
          completedAttemptDurations.push(Date.now() - attemptStartedAt)
          return {
            success: true,
            taskId,
            partialResults,
            metrics: {
              rowsScanned, rowsReturned, rowsPassedFilter, bytesScanned, bytesSkipped,
              bytesShuffledTotal: plan.shuffle?.bytesWritten
                ? Math.floor(Number(plan.shuffle.bytesWritten) / partitions.length) +
                  (partitionIndex < Number(plan.shuffle.bytesWritten) % partitions.length ? 1 : 0)
                : 0,
              peakMemoryBytes, cpuTimeMicros, workerId: worker.workerId
            },
            speculative
          }
        } catch (err) {
          lastError = err
          console.error(`[JobManager] Attempt ${attempt}/${MAX_PARTITION_ATTEMPTS} for partition ${partition.id} failed on ${workerAddress(worker)}:`, err.message)
          await db.query(
            `UPDATE tasks SET status = 'failed', completed_at = NOW(), error_message = $2 WHERE id = $1`,
            [taskId, err.message]
          )
          tasksCounter.add(1, { status: 'failed' })
          throw err
        } finally {
          worker.activeTasks = Math.max(0, (worker.activeTasks || 1) - 1)
        }
      }

      let winner = null
      let attemptsLaunched = 1
      const first = chooseWorker(1)
      const firstController = new AbortController()
      const speculativeController = new AbortController()
      const firstAttempt = runAttempt(1, first.worker, first.minimumLoad, false, firstController.signal)
      let raceResolved = false
      let speculativeRun = null
      const canSpeculate = getEligibleWorkers(plan, resourceBudget.workerLimit)
        .some(worker => worker.workerId !== first.worker.workerId)

      if (canSpeculate && resourceBudget.speculation !== false) {
        const firstStartedAt = Date.now()
        const speculativeAttempt = new Promise(resolve => setTimeout(resolve, SPECULATION_DELAY_MS))
          .then(async () => {
            const remaining = speculationThresholdMs(completedAttemptDurations, SPECULATION_DELAY_MS) - (Date.now() - firstStartedAt)
            if (remaining > 0) await new Promise(resolve => setTimeout(resolve, remaining))
            if (raceResolved) throw new Error('speculation no longer needed')
            attemptsLaunched = 2
            const choice = chooseWorker(2)
            pushToSubscribers(jobId, {
              type: 'speculative', partitionId: partition.id,
              originalWorker: first.worker.workerId, speculativeWorker: choice.worker.workerId
            })
            speculativeRun = runAttempt(2, choice.worker, choice.minimumLoad, true, speculativeController.signal)
            return speculativeRun
          })
        try {
          winner = await Promise.any([firstAttempt, speculativeAttempt])
        } catch (error) {
          lastError = error
        } finally {
          raceResolved = true
        }
        // A published winner must not leave a detached attempt capable of
        // mutating lineage after the job commit. Cancel the loser and wait for
        // its task row to reach a terminal state.
        firstController.abort()
        speculativeController.abort()
        await Promise.allSettled([firstAttempt, ...(speculativeRun ? [speculativeRun] : [])])
      } else {
        try { winner = await firstAttempt } catch (error) { lastError = error }
      }

      for (let attempt = attemptsLaunched + 1; !winner && attempt <= MAX_PARTITION_ATTEMPTS; attempt++) {
        const choice = chooseWorker(attempt)
        pushToSubscribers(jobId, { type: 'retry', partitionId: partition.id, nextAttempt: attempt })
        try {
          winner = await runAttempt(attempt, choice.worker, choice.minimumLoad)
        } catch (error) {
          lastError = error
        }
      }
      if (!winner) {
        throw new Error(`Partition ${partition.id} failed after ${MAX_PARTITION_ATTEMPTS} attempts: ${lastError?.message || 'unknown error'}`)
      }

      const logicalPartitionKey = partition.logicalPartitionKey || `partition-${partition.id}`
      const commitClient = await db.getClient()
      try {
        await commitClient.query('BEGIN')
        const committed = await commitClient.query(
          `UPDATE tasks SET is_winner = TRUE WHERE id = $1
           AND NOT EXISTS (
             SELECT 1 FROM tasks WHERE job_id = $2 AND logical_partition_key = $3 AND is_winner = TRUE
           ) RETURNING id`,
          [winner.taskId, jobId, logicalPartitionKey]
        )
        if (committed.rowCount !== 1) throw new Error(`Logical partition ${logicalPartitionKey} already has a committed winner`)
        const accumulatorValues = {
          rows_scanned: winner.metrics.rowsScanned,
          rows_returned: winner.metrics.rowsReturned,
          rows_passed_filter: winner.metrics.rowsPassedFilter,
          bytes_shuffled_total: winner.metrics.bytesShuffledTotal,
          bytes_scanned: winner.metrics.bytesScanned,
          cpu_time_micros: winner.metrics.cpuTimeMicros
        }
        for (const [name, value] of Object.entries(accumulatorValues)) {
          await commitClient.query(
            `INSERT INTO job_accumulators
               (job_id, logical_partition_key, name, numeric_value, committed_task_id)
             VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
            [jobId, logicalPartitionKey, name, Number(value || 0), winner.taskId]
          )
        }
        await commitClient.query('COMMIT')
      } catch (error) {
        await commitClient.query('ROLLBACK')
        throw error
      } finally {
        commitClient.release()
      }
      completedPartitions++
      pushToSubscribers(jobId, {
        type: 'progress', completedTasks: completedPartitions, totalTasks: partitions.length
      })
      return winner
    }

    const settledTasks = await Promise.allSettled(
      partitions.map((partition, index) => executePartition(partition, index))
    )
    const failures = settledTasks.filter(result => result.status === 'rejected')
    if (failures.length > 0) {
      throw new Error(`Query failed: ${failures.map(result => result.reason.message).join('; ')}`)
    }
    const taskResults = settledTasks.map(result => result.value)

    // ── 5. Merge only one committed winner per partition ───────────────────────
    const winnerCount = await db.query(
      'SELECT COUNT(*)::int AS count FROM tasks WHERE job_id = $1 AND is_winner = TRUE',
      [jobId]
    )
    if (winnerCount.rows[0].count !== partitions.length) {
      throw new Error(`Incomplete result: expected ${partitions.length} partition winners, found ${winnerCount.rows[0].count}`)
    }

    const allPartialResults = taskResults.flatMap(tr => tr.partialResults)
    const finalRows = mergeResults(allPartialResults, plan)
    if (finalRows.length > Number(resourceBudget.maxResultRows || 100000)) {
      throw new Error(`Result row budget exceeded: ${finalRows.length} > ${resourceBudget.maxResultRows}`)
    }
    const resultChecksum = crypto.createHash('sha256').update(JSON.stringify(finalRows)).digest('hex')

    // Persist before publication. This gives late subscribers a lossless replay
    // path and makes the completed job record the commit marker.
    const client = await db.getClient()
    const executionTimeMs = Date.now() - startMs
    try {
      await client.query('BEGIN')
      if (finalRows.length > 0) {
        await client.query(
          `INSERT INTO job_result_rows (job_id, row_index, row_data)
           SELECT $1, ordinality - 1, value::jsonb
           FROM unnest($2::text[]) WITH ORDINALITY AS rows(value, ordinality)`,
          [jobId, finalRows.map(row => JSON.stringify(row))]
        )
      }
      await client.query(
        `UPDATE jobs SET status = 'completed', completed_at = NOW(),
         result_row_count = $2, execution_time_ms = $3, result_checksum = $4 WHERE id = $1`,
        [jobId, finalRows.length, executionTimeMs, resultChecksum]
      )
      const transformNode = await client.query(
        `UPDATE lineage_nodes SET status = 'available', metadata_json = metadata_json || $2::jsonb
         WHERE job_id = $1 AND logical_partition_key = '__job__' RETURNING id`,
        [jobId, JSON.stringify({ resultChecksum, resultRows: finalRows.length })]
      )
      const winners = await client.query(
          `SELECT id, partition_id, logical_partition_key, rows_processed, transferred_bytes, partial_result_checksum
         FROM tasks WHERE job_id = $1 AND is_winner = TRUE`, [jobId]
      )
      for (const task of winners.rows) {
        const node = await client.query(
          `INSERT INTO lineage_nodes
             (kind, operator, partition_id, job_id, logical_partition_key, metadata_json, status)
           VALUES ('partition', 'QUERY_PARTITION', $1,$2,$3,$4,'available')
           ON CONFLICT (job_id, logical_partition_key) DO UPDATE
             SET metadata_json = EXCLUDED.metadata_json, status = 'available'
           RETURNING id`,
          [task.partition_id, jobId, task.logical_partition_key,
            JSON.stringify({ taskId: task.id, rows: task.rows_processed, transferredBytes: task.transferred_bytes,
              partialResultChecksum: task.partial_result_checksum })]
        )
        await client.query(
          `INSERT INTO lineage_edges (parent_id, child_id, edge_type)
           VALUES ($1,$2,'produces') ON CONFLICT DO NOTHING`,
          [transformNode.rows[0].id, node.rows[0].id]
        )
      }
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    } finally {
      client.release()
    }

    const timing = await db.query(
      `SELECT EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000 AS duration_ms
       FROM tasks WHERE job_id = $1 AND is_winner = TRUE AND completed_at IS NOT NULL`, [jobId]
    )
    const durations = timing.rows.map(row => Number(row.duration_ms || 0)).sort((a, b) => a - b)
    const medianDuration = durations.length ? durations[Math.floor(durations.length / 2)] : 0
    const stragglerRatio = medianDuration > 0 ? Math.max(...durations) / medianDuration : 1
    const hotBucketCount = Number(plan.shuffle?.hotBuckets?.length || 0)
    const recommendedBuckets = plan.joinStrategy === 'hash_shuffle'
      ? Math.min(8, Math.max(1, hotBucketCount ? Number(plan.shuffle.buckets) * 2 : Number(plan.shuffle.buckets)))
      : null
    await db.query(
      `INSERT INTO query_feedback
         (fingerprint, executions, ema_result_rows, ema_straggler_ratio,
          recommended_shuffle_buckets, hot_bucket_count, last_plan_json)
       VALUES ($1,1,$2,$3,$4,$5,$6)
       ON CONFLICT (fingerprint) DO UPDATE SET
         executions = query_feedback.executions + 1,
         ema_result_rows = query_feedback.ema_result_rows * 0.7 + EXCLUDED.ema_result_rows * 0.3,
         ema_straggler_ratio = query_feedback.ema_straggler_ratio * 0.7 + EXCLUDED.ema_straggler_ratio * 0.3,
         recommended_shuffle_buckets = EXCLUDED.recommended_shuffle_buckets,
         hot_bucket_count = EXCLUDED.hot_bucket_count,
         last_plan_json = EXCLUDED.last_plan_json,
         updated_at = NOW()`,
      [queryFingerprint, finalRows.length, stragglerRatio, recommendedBuckets, hotBucketCount, JSON.stringify(plan)]
    )

    // ── 6. Publish committed rows to connected subscribers ───────────────────
    for (const row of finalRows) pushToSubscribers(jobId, { type: 'row', data: row })

    queryDurationHistogram.record(executionTimeMs)
    span.setAttributes({ 'job.result_rows': finalRows.length, 'job.duration_ms': executionTimeMs })

    const workerMetrics = taskResults.map(tr => ({
      workerId: tr.metrics.workerId,
      rowsReturned: tr.metrics.rowsReturned,
      rowsScanned: tr.metrics.rowsScanned,
      bytesScanned: tr.metrics.bytesScanned,
      bytesSkipped: tr.metrics.bytesSkipped,
      peakMemoryBytes: tr.metrics.peakMemoryBytes,
      cpuTimeMicros: tr.metrics.cpuTimeMicros,
      success: true
    }))

    pushToSubscribers(jobId, {
      type: 'complete', totalRows: finalRows.length, executionTimeMs, workerMetrics
    })

    console.log(`[JobManager] Job ${jobId} completed in ${executionTimeMs}ms — ${finalRows.length} rows`)
    return { jobId }
  } catch (err) {
    span.recordException(err)
    // Mark job as failed
    try {
      await db.query(
        `UPDATE jobs SET status = 'failed', completed_at = NOW() WHERE id = $1 AND status != 'completed'`,
        [jobId]
      )
      await db.query(`UPDATE lineage_nodes SET status = 'failed' WHERE job_id = $1`, [jobId])
    } catch (dbErr) {
      console.error('[JobManager] Failed to mark job as failed:', dbErr.message)
    }
    pushToSubscribers(jobId, { type: 'error', message: err.message })
    throw err
  } finally {
    span.end()
  }
}

async function recoverInterruptedJobs (startedBefore = new Date()) {
  const interrupted = await db.query(
    `SELECT id, sql_query, dataset_id, resource_budget_json, query_context_json
     FROM jobs WHERE status = 'running' AND created_at < $1 ORDER BY created_at`,
    [startedBefore]
  )
  for (const job of interrupted.rows) {
    const context = job.query_context_json || {}
    const budget = { ...(job.resource_budget_json || {}), resumeExisting: true }
    executeQuery(job.sql_query, job.dataset_id, job.id, context.datasetIds || {}, budget)
      .catch(error => console.error(`[Recovery] Job ${job.id} failed:`, error.message))
  }
  return interrupted.rowCount
}

module.exports = { executeQuery, recoverInterruptedJobs }
