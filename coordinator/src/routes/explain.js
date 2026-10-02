'use strict'

const express               = require('express')
const db                    = require('../db')
const { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas } = require('../services/queryPlanner')
const { chooseJoinStrategy } = require('../services/adaptiveJoin')
const { explorePartitioning } = require('../services/partitionExplorer')
const { matchesMeasuredExecution } = require('../services/explainEvidence')

const router = express.Router()

router.post('/partition', async (req, res) => {
  try {
    res.json(await explorePartitioning(req.body || {}))
  } catch (error) {
    const clientError = /required|must be|Unknown column|not found|Parquet snapshot/.test(error.message)
    res.status(clientError ? 400 : 503).json({ error: error.message })
  }
})

/**
 * POST /api/explain
 * Body: { sql: string, datasetId: string }
 *
 * Returns the execution plan as JSON — shows exactly what will happen
 * before running the query. Like PostgreSQL's EXPLAIN.
 */
router.post('/', async (req, res) => {
  const { sql, datasetId, datasetIds = {}, resourceBudget = {}, jobId = null } = req.body

  if (!sql || typeof sql !== 'string') {
    return res.status(400).json({ error: 'sql is required' })
  }
  if (!datasetId) {
    return res.status(400).json({ error: 'datasetId is required' })
  }

  // Parse SQL → execution plan
  let plan
  try {
    plan = buildExecutionPlan(sql)
  } catch (err) {
    return res.status(400).json({ error: `SQL parse error: ${err.message}` })
  }

  // Look up dataset metadata
  let dataset, partitions, joinedDataset = null, joinedPartitions = []
  try {
    const dsResult = await db.query('SELECT * FROM datasets WHERE id = $1', [datasetId])
    if (dsResult.rows.length === 0) {
      return res.status(404).json({ error: `Dataset ${datasetId} not found` })
    }
    dataset = dsResult.rows[0]

    const partResult = await db.query(
      'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index',
      [datasetId]
    )
    partitions = partResult.rows
    if (plan.join) {
      const table = plan.tables[1]
      const joinedId = datasetIds[table.name]
      if (!joinedId) return res.status(400).json({ error: `datasetIds.${table.name} is required` })
      const joinedResult = await db.query('SELECT * FROM datasets WHERE id = $1', [joinedId])
      if (joinedResult.rowCount === 0) return res.status(404).json({ error: `Joined dataset ${joinedId} not found` })
      joinedDataset = joinedResult.rows[0]
      joinedPartitions = (await db.query(
        'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index', [joinedId]
      )).rows
      validateJoinPlanAgainstSchemas(plan, [dataset, joinedDataset])
    } else {
      validatePlanAgainstSchema(plan, dataset.schema_json)
    }
  } catch (err) {
    return res.status(500).json({ error: err.message })
  }

  // Estimate predicate selectivity from schema
  const schema = dataset.schema_json || { columns: [] }
  const totalRows = dataset.row_count || 0

  // Build the explain output
  const primaryBytes = partitions.reduce((sum, item) => sum + Number(item.parquet_byte_size || item.csv_byte_size || 0), 0)
  const secondaryBytes = joinedPartitions.reduce((sum, item) => sum + Number(item.parquet_byte_size || item.csv_byte_size || 0), 0)
  const buildSide = plan.join ? (primaryBytes <= secondaryBytes ? plan.tables[0] : plan.tables[1]) : null
  const probeSide = plan.join ? (primaryBytes <= secondaryBytes ? plan.tables[1] : plan.tables[0]) : null
  const adaptive = plan.join ? chooseJoinStrategy(primaryBytes, secondaryBytes) : null
  const explainOutput = {
    query: sql,
    dataset: {
      name:            dataset.name,
      total_rows:      totalRows,
      partition_count: partitions.length
    },
    execution_plan: {
      operation:     plan.join ? `${adaptive.strategy.toUpperCase()}_HASH_JOIN` : plan.aggregations.length > 0 ? 'DISTRIBUTED_AGGREGATE' : 'DISTRIBUTED_SCAN',
      table:         plan.tableName,
      workers:       partitions.length,
      rows_per_worker: Math.ceil(totalRows / partitions.length)
    },
    mapreduce: {
      combiner: plan.aggregations.length > 0 && resourceBudget.combiner !== false,
      combiner_operator: plan.aggregations.length > 0 ? {
        name: 'LocalCombiner', enabled: resourceBudget.combiner !== false,
        states: Object.fromEntries(plan.aggregations.map(aggregation => [
          aggregation.alias,
          aggregation.function === 'AVG' ? ['sum', 'count'] : [aggregation.function.toLowerCase(), 'count']
        ]))
      } : null,
      mapper_output: plan.aggregations.length > 0 && resourceBudget.combiner === false
        ? 'one mergeable aggregate state per input row'
        : plan.aggregations.length > 0 ? 'one mergeable aggregate state per local group' : 'projected result rows',
      partition_function: plan.join ? `hash(${plan.join.leftColumn}) mod R` : 'physical dataset partitions',
      speculative_execution: resourceBudget.speculation === false
        ? 'disabled'
        : 'enabled; threshold=max(250ms, 1.5× sibling p75)'
    },
    cost_model: {
      estimated_input_bytes: primaryBytes + secondaryBytes,
      estimated_shuffle_bytes: plan.join && adaptive.strategy === 'hash_shuffle' ? primaryBytes + secondaryBytes : 0,
      estimated_combiner_output_rows: plan.aggregations.length
        ? (resourceBudget.combiner === false ? totalRows : Math.min(totalRows, Math.max(1, partitions.length) * 1000))
        : totalRows,
      equation: 'cost = scan_bytes + shuffle_bytes + combiner_output_rows × state_width',
      caveat: 'Static estimate from catalog metadata; Query Autopsy reports measured scan, transfer, CPU, memory, and critical path.'
    },
    adaptive_join: plan.join ? {
      strategy: adaptive.strategy,
      reason: adaptive.reason,
      build_side: buildSide.name,
      build_bytes: Math.min(primaryBytes, secondaryBytes),
      probe_side: probeSide.name,
      probe_bytes: Math.max(primaryBytes, secondaryBytes)
    } : null,
    operator_dag: plan.join ? {
      nodes: [
        { id: 'build_scan', operator: 'ParquetScan', table: buildSide.name, estimated_bytes: Math.min(primaryBytes, secondaryBytes) },
        { id: 'probe_scan', operator: 'ParquetScan', table: probeSide.name, estimated_bytes: Math.max(primaryBytes, secondaryBytes) },
        ...(adaptive.strategy === 'hash_shuffle' ? [{ id: 'shuffle', operator: 'HashExchange', partitions: Math.max(1, partitions.length) }] : []),
        { id: 'hash_join', operator: adaptive.strategy === 'broadcast' ? 'BroadcastHashJoin' : adaptive.strategy === 'local' ? 'LocalHashJoin' : 'PartitionedHashJoin', condition: `${plan.join.leftAlias}.${plan.join.leftColumn} = ${plan.join.rightAlias}.${plan.join.rightColumn}` },
        ...(plan.aggregations.length ? [{
          id: 'partial_aggregate', operator: 'LocalCombiner', group_by: plan.groupByColumns,
          enabled: resourceBudget.combiner !== false,
          state: plan.aggregations.map(item => item.function === 'AVG' ? `${item.alias}:(sum,count)` : `${item.alias}:${item.function.toLowerCase()}`)
        }] : []),
        { id: 'coordinator_merge', operator: plan.aggregations.length ? 'FinalAggregate' : 'ResultMerge' }
      ],
      edges: [
        { from: 'build_scan', to: adaptive.strategy === 'hash_shuffle' ? 'shuffle' : 'hash_join', distribution: adaptive.strategy },
        { from: 'probe_scan', to: adaptive.strategy === 'hash_shuffle' ? 'shuffle' : 'hash_join', distribution: adaptive.strategy === 'local' ? 'local' : 'partitioned' },
        ...(adaptive.strategy === 'hash_shuffle' ? [{ from: 'shuffle', to: 'hash_join', distribution: 'hash_partitioned' }] : []),
        { from: 'hash_join', to: plan.aggregations.length ? 'partial_aggregate' : 'coordinator_merge' },
        ...(plan.aggregations.length ? [{ from: 'partial_aggregate', to: 'coordinator_merge' }] : [])
      ]
    } : {
      nodes: [
        { id: 'partition_map', operator: 'PartitionMap', partitions: partitions.length, predicates: plan.predicates },
        ...(plan.aggregations.length ? [{
          id: 'local_combiner', operator: 'LocalCombiner', enabled: resourceBudget.combiner !== false,
          state: plan.aggregations.map(item => item.function === 'AVG' ? `${item.alias}:(sum,count)` : `${item.alias}:${item.function.toLowerCase()}`)
        }] : []),
        { id: 'coordinator_reduce', operator: plan.aggregations.length ? 'FinalReduce' : 'ResultMerge' }
      ],
      edges: plan.aggregations.length
        ? [
            { from: 'partition_map', to: 'local_combiner', distribution: 'worker-local', bypass: resourceBudget.combiner === false },
            { from: 'local_combiner', to: 'coordinator_reduce', distribution: 'gRPC mergeable state', bypass: resourceBudget.combiner === false }
          ]
        : [{ from: 'partition_map', to: 'coordinator_reduce', distribution: 'Arrow IPC' }]
    },
    predicate_pushdown: {
      enabled:    plan.predicates.length > 0,
      predicates: plan.predicates.map(p => ({
        column:   p.column,
        operator: p.operator,
        value:    p.value,
        type:     p.type,
        note:     'Applied row-by-row on worker BEFORE loading into memory'
      })),
      estimated_benefit: plan.predicates.length > 0
        ? 'Rows not matching WHERE are discarded immediately — never transferred to coordinator'
        : 'No WHERE clause — all rows will be transferred'
    },
    aggregation: {
      type: plan.groupByColumns.length > 0 ? 'PARTIAL_AGGREGATION' : 'NONE',
      group_by: plan.groupByColumns,
      functions: plan.aggregations.map(a => ({
        function:    a.function,
        column:      a.column,
        alias:       a.alias,
        worker_does: a.function === 'AVG'
          ? 'Computes local SUM + COUNT (never sends AVG directly)'
          : a.function === 'MAX' || a.function === 'MIN'
          ? `Computes local ${a.function} on its partition`
          : `Computes local ${a.function}`,
        coordinator_does: a.function === 'AVG'
          ? 'Merges: final_avg = total_sum / total_count across all workers'
          : a.function === 'MAX'
          ? 'Takes MAX of all worker MAX values'
          : a.function === 'MIN'
          ? 'Takes MIN of all worker MIN values'
          : 'Sums all worker partial results'
      })),
      note: plan.groupByColumns.length > 0
        ? 'Each worker builds a local hash map. Coordinator merges N hash maps — NOT N×rows'
        : null
    },
    ordering: plan.orderByColumn
      ? { column: plan.orderByColumn, direction: plan.orderByDirection, applied_at: 'coordinator — after all workers complete' }
      : null,
    limit: plan.limit > 0
      ? { value: plan.limit, applied_at: 'coordinator — after ORDER BY on merged results' }
      : null,
    partitions: partitions.map(p => ({
      index:      p.partition_index,
      minio_path: p.minio_path,
      row_count:  p.row_count,
      worker:     `worker-${p.partition_index + 1}`
    }))
  }

  if (jobId) {
    const measuredJob = await db.query(
      `SELECT id, dataset_id, sql_query, query_context_json, status,
              result_row_count, execution_time_ms, plan_json
       FROM jobs WHERE id=$1`, [jobId]
    )
    if (measuredJob.rowCount === 0 || measuredJob.rows[0].dataset_id !== datasetId ||
        !matchesMeasuredExecution(measuredJob.rows[0], sql, datasetIds)) {
      return res.status(400).json({ error: 'jobId must reference this SQL and the same dataset snapshots' })
    }
    if (measuredJob.rows[0].status !== 'completed') {
      return res.status(409).json({ error: 'Measured EXPLAIN requires a completed job' })
    }
    const measuredTasks = (await db.query(
      `SELECT *, EXTRACT(EPOCH FROM (completed_at-started_at))*1000 AS duration_ms
       FROM tasks WHERE job_id=$1 ORDER BY started_at`, [jobId]
    )).rows
    const winners = measuredTasks.filter(task => task.is_winner)
    const sum = (items, field) => items.reduce((total, item) => total + Number(item[field] || 0), 0)
    explainOutput.measured_cost = {
      job_id: jobId,
      input_rows: sum(winners, 'rows_scanned'),
      input_bytes: sum(winners, 'bytes_scanned'),
      shuffle_bytes: Number(measuredJob.rows[0].plan_json?.shuffle?.bytesWritten || 0),
      output_rows: Number(measuredJob.rows[0].result_row_count || 0),
      output_wire_bytes: sum(winners, 'transferred_bytes'),
      cpu_time_micros: sum(measuredTasks, 'cpu_time_micros'),
      critical_path_ms: Math.max(0, ...measuredTasks.map(task => Number(task.duration_ms || 0))),
      elapsed_ms: Number(measuredJob.rows[0].execution_time_ms || 0),
      equations: {
        communication: 'C_comm = shuffle_bytes + output_wire_bytes',
        work: 'T_1(measured) = Σ attempt_cpu_time',
        critical_path: 'T_∞(measured) = max attempt_wall_time',
        parallelism_bound: 'speedup ≤ T_1 / T_∞'
      },
      scope: 'Input/output counters use logical winners; CPU and critical path include failed/speculative work.'
    }
  }

  res.json(explainOutput)
})

module.exports = router
