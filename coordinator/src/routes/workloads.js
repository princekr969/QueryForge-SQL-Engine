'use strict'

const express = require('express')
const crypto = require('crypto')
const db = require('../db')
const { executeQuery } = require('../services/jobManager')

const router = express.Router()
const OVERRIDES = new Set(['workerLimit', 'combiner', 'speculation', 'cacheLevel', 'cacheBudgetBytes', 'joinStrategy', 'skewSplitFactor', 'maxExecutionMs'])

router.post('/', async (req, res) => {
  const { name, description = '', jobIds } = req.body || {}
  if (!name || !Array.isArray(jobIds) || jobIds.length === 0 || jobIds.length > 100) {
    return res.status(400).json({ error: 'name and 1-100 jobIds are required' })
  }
  const client = await db.getClient()
  try {
    await client.query('BEGIN')
    const workload = await client.query(
      'INSERT INTO workloads (name, description) VALUES ($1,$2) RETURNING *', [name, description]
    )
    for (let index = 0; index < jobIds.length; index++) {
      const job = await client.query('SELECT * FROM jobs WHERE id = $1 AND status = $2', [jobIds[index], 'completed'])
      if (job.rowCount === 0) throw new Error(`Completed job not found: ${jobIds[index]}`)
      const item = job.rows[0]
      await client.query(
        `INSERT INTO workload_queries
           (workload_id, sequence_number, source_job_id, sql_query, dataset_id,
            dataset_ids_json, resource_budget_json, baseline_checksum, baseline_latency_ms, baseline_plan_json)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [workload.rows[0].id, index, item.id, item.sql_query, item.dataset_id,
          JSON.stringify(item.query_context_json?.datasetIds || {}), JSON.stringify(item.resource_budget_json || {}),
          item.result_checksum, item.execution_time_ms, JSON.stringify(item.plan_json || {})]
      )
    }
    await client.query('COMMIT')
    res.status(201).json({ ...workload.rows[0], queryCount: jobIds.length })
  } catch (error) {
    await client.query('ROLLBACK')
    res.status(400).json({ error: error.message })
  } finally { client.release() }
})

router.get('/:id', async (req, res) => {
  const workload = await db.query('SELECT * FROM workloads WHERE id = $1', [req.params.id])
  if (workload.rowCount === 0) return res.status(404).json({ error: 'Workload not found' })
  const queries = await db.query('SELECT * FROM workload_queries WHERE workload_id = $1 ORDER BY sequence_number', [req.params.id])
  const replays = await db.query('SELECT * FROM workload_replays WHERE workload_id = $1 ORDER BY created_at DESC', [req.params.id])
  res.json({ workload: workload.rows[0], queries: queries.rows, replays: replays.rows })
})

router.post('/:id/replay', async (req, res) => {
  try {
    const input = req.body || {}
    const overrides = input.overrides || {}
    if (Object.keys(overrides).some(key => !OVERRIDES.has(key))) throw new Error(`overrides may use: ${[...OVERRIDES].join(', ')}`)
    const queries = await db.query('SELECT * FROM workload_queries WHERE workload_id = $1 ORDER BY sequence_number', [req.params.id])
    if (queries.rowCount === 0) return res.status(404).json({ error: 'Workload not found or empty' })
    const results = []
    for (const query of queries.rows) {
      const jobId = crypto.randomUUID()
      await executeQuery(query.sql_query, query.dataset_id, jobId, query.dataset_ids_json || {}, {
        ...(query.resource_budget_json || {}), ...overrides
      })
      const replayed = (await db.query('SELECT * FROM jobs WHERE id = $1', [jobId])).rows[0]
      results.push({
        sequenceNumber: query.sequence_number, jobId,
        checksumMatch: replayed.result_checksum === query.baseline_checksum,
        baselineLatencyMs: query.baseline_latency_ms, replayLatencyMs: replayed.execution_time_ms,
        latencyDelta: query.baseline_latency_ms ? replayed.execution_time_ms / query.baseline_latency_ms - 1 : null,
        baselineStrategy: query.baseline_plan_json?.joinStrategy || 'partition_scan',
        replayStrategy: replayed.plan_json?.joinStrategy || 'partition_scan',
        baselineResultRows: query.baseline_plan_json?.runtimeFeedback?.emaResultRows || null,
        replayResultRows: replayed.result_row_count
      })
    }
    const saved = await db.query(
      `INSERT INTO workload_replays (workload_id, overrides_json, result_json, status, completed_at)
       VALUES ($1,$2,$3,'completed',NOW()) RETURNING id, created_at`,
      [req.params.id, JSON.stringify(overrides), JSON.stringify(results)]
    )
    res.json({ replayId: saved.rows[0].id, workloadId: req.params.id, overrides, results })
  } catch (error) { res.status(400).json({ error: error.message }) }
})

module.exports = router
