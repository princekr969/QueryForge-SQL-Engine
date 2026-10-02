'use strict'

const express          = require('express')
const { v4: uuidv4 }   = require('uuid')
const db               = require('../db')
const { executeQuery } = require('../services/jobManager')
const { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas } = require('../services/queryPlanner')
const { admissionController } = require('../services/admissionController')
const { buildAutopsy } = require('../services/queryAutopsy')
const { predictWhatIf } = require('../services/whatIfAnalyzer')
const crypto = require('crypto')

const router = express.Router()

// POST /api/query — submit a SQL query, returns jobId immediately
// Client subscribes via WebSocket BEFORE results stream in
router.post('/', async (req, res) => {
  const { sql, datasetId, datasetIds = {}, priority = 0, resourceBudget = {} } = req.body

  if (!sql || typeof sql !== 'string') {
    return res.status(400).json({ error: 'sql is required' })
  }

  if (!datasetId) {
    return res.status(400).json({ error: 'datasetId is required' })
  }
  if (!Number.isInteger(priority) || priority < -10 || priority > 10) {
    return res.status(400).json({ error: 'priority must be an integer from -10 to 10' })
  }
  const maxExecutionMs = Number(resourceBudget.maxExecutionMs || 30000)
  const maxResultRows = Number(resourceBudget.maxResultRows || 100000)
  const workerLimit = Number(resourceBudget.workerLimit || 0)
  const skewSplitFactor = Number(resourceBudget.skewSplitFactor || 1.5)
  const joinStrategy = resourceBudget.joinStrategy || 'auto'
  const combiner = resourceBudget.combiner !== false
  const speculation = resourceBudget.speculation !== false
  const cacheLevel = String(resourceBudget.cacheLevel || 'NONE').toUpperCase()
  const cacheBudgetBytes = Number(resourceBudget.cacheBudgetBytes || 256 * 1024 * 1024)
  if (!Number.isFinite(maxExecutionMs) || maxExecutionMs < 100 || maxExecutionMs > 300000 ||
      !Number.isSafeInteger(maxResultRows) || maxResultRows < 1 || maxResultRows > 1000000 ||
      !Number.isSafeInteger(workerLimit) || workerLimit < 0 || workerLimit > 32 ||
      !Number.isFinite(skewSplitFactor) || skewSplitFactor < 1 || skewSplitFactor > 100 ||
      !['auto', 'local', 'broadcast', 'hash_shuffle'].includes(joinStrategy) ||
      (resourceBudget.combiner !== undefined && typeof resourceBudget.combiner !== 'boolean') ||
      (resourceBudget.speculation !== undefined && typeof resourceBudget.speculation !== 'boolean') ||
      !['NONE', 'MEMORY', 'DISK', 'MEMORY_AND_DISK'].includes(cacheLevel) ||
      !Number.isSafeInteger(cacheBudgetBytes) || cacheBudgetBytes < 1024 * 1024 || cacheBudgetBytes > 4 * 1024 * 1024 * 1024) {
    return res.status(400).json({ error: 'Invalid resourceBudget limits' })
  }

  // Validate dataset exists before fire-and-forget
  try {
    const plan = buildExecutionPlan(sql)
    const dsCheck = await db.query('SELECT id, name, schema_json FROM datasets WHERE id = $1', [datasetId])
    if (dsCheck.rows.length === 0) {
      return res.status(404).json({ error: `Dataset ${datasetId} not found` })
    }
    if (String(plan.tableName).toLowerCase() !== String(dsCheck.rows[0].name).toLowerCase()) {
      return res.status(400).json({
        error: `FROM table "${plan.tableName}" does not match selected dataset "${dsCheck.rows[0].name}"`
      })
    }
    if (plan.join) {
      const joined = [dsCheck.rows[0]]
      for (const table of plan.tables.slice(1)) {
        const secondaryId = datasetIds[table.name]
        if (!secondaryId) throw new Error(`datasetIds.${table.name} is required for the joined table`)
        const result = await db.query('SELECT id, name, schema_json FROM datasets WHERE id = $1', [secondaryId])
        if (result.rowCount === 0 || String(result.rows[0].name).toLowerCase() !== table.name.toLowerCase()) {
          throw new Error(`datasetIds.${table.name} does not reference table ${table.name}`)
        }
        joined.push(result.rows[0])
      }
      validateJoinPlanAgainstSchemas(plan, joined)
    } else {
      validatePlanAgainstSchema(plan, dsCheck.rows[0].schema_json)
    }
  } catch (err) {
    console.error('[Route POST /query] Validation error:', err.message)
    return res.status(400).json({ error: err.message })
  }

  // Pre-generate jobId so we can return it immediately.
  // Client subscribes to this jobId via WebSocket right after getting this response.
  const jobId = uuidv4()

  // Fire-and-forget — results stream to WebSocket subscribers
  let admission
  try {
    admission = admissionController.admit({
      jobId,
      priority,
      run: () => executeQuery(sql, datasetId, jobId, datasetIds, {
        priority, maxExecutionMs, maxResultRows, workerLimit, skewSplitFactor, joinStrategy,
        combiner, speculation, cacheLevel, cacheBudgetBytes
      })
    })
  } catch (err) {
    return res.status(429).json({ error: err.message })
  }
  admission.promise.catch(err => {
    console.error(`[Route POST /query] Job ${jobId} failed:`, err.message)
  })

  res.status(202).json({ jobId, priority, queuePosition: admission.position })
})

router.get('/admission', (_req, res) => res.json(admissionController.snapshot()))

router.get('/jobs/:id/autopsy', async (req, res) => {
  try {
    const job = await db.query('SELECT * FROM jobs WHERE id = $1', [req.params.id])
    if (job.rowCount === 0) return res.status(404).json({ error: 'Job not found' })
    const tasks = await db.query('SELECT * FROM tasks WHERE job_id = $1 ORDER BY started_at', [req.params.id])
    res.json(buildAutopsy(job.rows[0], tasks.rows))
  } catch (error) {
    res.status(500).json({ error: error.message })
  }
})

router.get('/jobs/:id/accumulators', async (req, res) => {
  try {
    const values = await db.query(
      `SELECT name, SUM(numeric_value)::double precision AS value,
       COUNT(*)::int AS committed_partitions
       FROM job_accumulators WHERE job_id = $1 GROUP BY name ORDER BY name`, [req.params.id]
    )
    res.json({ jobId: req.params.id, accumulators: values.rows })
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.post('/jobs/:id/what-if', async (req, res) => {
  try {
    const job = await db.query('SELECT * FROM jobs WHERE id = $1', [req.params.id])
    if (job.rowCount === 0) return res.status(404).json({ error: 'Job not found' })
    if (job.rows[0].status !== 'completed') return res.status(409).json({ error: 'What-if analysis requires a completed job' })
    const tasks = await db.query('SELECT * FROM tasks WHERE job_id = $1', [req.params.id])
    const projection = predictWhatIf(job.rows[0], tasks.rows, req.body || {})
    if (req.body?.execute !== true) return res.json({ projection })
    const original = job.rows[0]
    const context = original.query_context_json || {}
    const overrides = {
      ...(original.resource_budget_json || {}),
      workerLimit: projection.inputs.targetWorkers,
      combiner: projection.inputs.targetCombiner,
      joinStrategy: projection.inputs.targetJoinStrategy || 'auto',
      ...(projection.inputs.cacheWarm ? { cacheLevel: 'MEMORY_AND_DISK' } : {})
    }
    const comparisonJobId = crypto.randomUUID()
    await executeQuery(original.sql_query, original.dataset_id, comparisonJobId, context.datasetIds || {}, overrides)
    const comparedJob = (await db.query('SELECT * FROM jobs WHERE id=$1', [comparisonJobId])).rows[0]
    const comparedTasks = (await db.query('SELECT * FROM tasks WHERE job_id=$1 AND is_winner=TRUE', [comparisonJobId])).rows
    const metric = (items, field, mode = 'sum') => mode === 'max'
      ? Math.max(0, ...items.map(item => Number(item[field] || 0)))
      : items.reduce((sum, item) => sum + Number(item[field] || 0), 0)
    const baselineWinners = tasks.rows.filter(item => item.is_winner)
    res.json({
      projection,
      measured: {
        jobId: comparisonJobId, checksumMatch: comparedJob.result_checksum === original.result_checksum,
        baseline: { latencyMs: original.execution_time_ms, bytes: metric(baselineWinners, 'transferred_bytes'), cpuMicros: metric(baselineWinners, 'cpu_time_micros'), peakMemoryBytes: metric(baselineWinners, 'peak_memory_bytes', 'max') },
        comparison: { latencyMs: comparedJob.execution_time_ms, bytes: metric(comparedTasks, 'transferred_bytes'), cpuMicros: metric(comparedTasks, 'cpu_time_micros'), peakMemoryBytes: metric(comparedTasks, 'peak_memory_bytes', 'max') }
      }
    })
  } catch (error) { res.status(400).json({ error: error.message }) }
})

// GET /api/query/jobs/:id — job status + task details
router.get('/jobs/:id', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM jobs WHERE id = $1', [req.params.id])
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Job not found' })
    }

    const tasksResult = await db.query(
      'SELECT * FROM tasks WHERE job_id = $1 ORDER BY started_at',
      [req.params.id]
    )
    const chaosResult = await db.query(
      'SELECT mode, task_id, configuration_json, created_at FROM chaos_events WHERE job_id = $1 ORDER BY created_at',
      [req.params.id]
    )
    const cacheResult = await db.query(
      'SELECT * FROM cache_events WHERE job_id = $1 ORDER BY created_at', [req.params.id]
    )

    res.json({ job: result.rows[0], tasks: tasksResult.rows, chaosEvents: chaosResult.rows, cacheEvents: cacheResult.rows })
  } catch (err) {
    console.error('[Route GET /query/jobs/:id] Error:', err.message)
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
