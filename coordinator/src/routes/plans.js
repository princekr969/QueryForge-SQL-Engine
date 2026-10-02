'use strict'

const express = require('express')
const crypto = require('crypto')
const fs = require('fs/promises')
const os = require('os')
const path = require('path')
const { stringify } = require('csv-stringify/sync')
const db = require('../db')
const { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas } = require('../services/queryPlanner')
const { executeQuery } = require('../services/jobManager')
const { partitionAndStore } = require('../services/partitioner')

const router = express.Router()
const TRANSFORMATIONS = new Set(['filter', 'map', 'select', 'groupBy', 'aggregate', 'join'])
const ACTIONS = new Set(['collect', 'count', 'write', 'materialize'])

async function validateLazyPlan ({ datasetId, datasetIds = {}, sql, transformations = [] }) {
  if (!datasetId || typeof datasetId !== 'string') throw new Error('datasetId is required')
  if (!sql || typeof sql !== 'string') throw new Error('sql is required')
  if (!Array.isArray(transformations) || transformations.some(item => !TRANSFORMATIONS.has(item.operation))) {
    throw new Error(`transformations must use: ${[...TRANSFORMATIONS].join(', ')}`)
  }
  const plan = buildExecutionPlan(sql)
  const primary = await db.query('SELECT id, name, schema_json FROM datasets WHERE id = $1', [datasetId])
  if (primary.rowCount === 0) throw new Error('Dataset not found')
  if (String(plan.tableName).toLowerCase() !== String(primary.rows[0].name).toLowerCase()) throw new Error('SQL table does not match dataset')
  if (plan.join) {
    const datasets = [primary.rows[0]]
    for (const table of plan.tables.slice(1)) {
      const bound = await db.query('SELECT id, name, schema_json FROM datasets WHERE id = $1', [datasetIds[table.name]])
      if (bound.rowCount === 0) throw new Error(`Dataset for ${table.name} not found`)
      datasets.push(bound.rows[0])
    }
    validateJoinPlanAgainstSchemas(plan, datasets)
  } else validatePlanAgainstSchema(plan, primary.rows[0].schema_json)
  return plan
}

router.post('/', async (req, res) => {
  try {
    const input = req.body || {}
    const plan = await validateLazyPlan(input)
    const saved = await db.query(
      `INSERT INTO lazy_plans
         (dataset_id, sql_query, dataset_ids_json, transformations_json, plan_json)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [input.datasetId, input.sql, JSON.stringify(input.datasetIds || {}),
        JSON.stringify(input.transformations || []), JSON.stringify(plan)]
    )
    res.status(201).json({ ...saved.rows[0], lazy: true, executed: false })
  } catch (error) {
    res.status(400).json({ error: error.message })
  }
})

router.get('/:id', async (req, res) => {
  const result = await db.query('SELECT * FROM lazy_plans WHERE id = $1', [req.params.id])
  if (result.rowCount === 0) return res.status(404).json({ error: 'Lazy plan not found' })
  res.json(result.rows[0])
})

router.post('/:id/actions/:action', async (req, res) => {
  const action = req.params.action
  if (!ACTIONS.has(action)) return res.status(400).json({ error: `action must be one of: ${[...ACTIONS].join(', ')}` })
  try {
    const stored = await db.query('SELECT * FROM lazy_plans WHERE id = $1', [req.params.id])
    if (stored.rowCount === 0) return res.status(404).json({ error: 'Lazy plan not found' })
    const lazy = stored.rows[0]
    const jobId = crypto.randomUUID()
    await db.query(
      `UPDATE lazy_plans SET status = 'running', last_action = $2,
       updated_at = NOW() WHERE id = $1`, [lazy.id, action]
    )
    await executeQuery(lazy.sql_query, lazy.dataset_id, jobId, lazy.dataset_ids_json || {}, req.body?.resourceBudget || {})
    const result = await db.query('SELECT row_data FROM job_result_rows WHERE job_id = $1 ORDER BY row_index', [jobId])
    const rows = result.rows.map(item => item.row_data)
    let materializedDataset = null
    if (action === 'write' || action === 'materialize') {
      if (rows.length === 0) throw new Error('Cannot materialize an empty result')
      const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'queryforge-materialize-'))
      const outputPath = path.join(directory, 'result.csv')
      try {
        await fs.writeFile(outputPath, stringify(rows, { header: true }))
        materializedDataset = await partitionAndStore(
          outputPath,
          `${String(req.body?.name || `materialized_${lazy.id}`).replace(/[^a-z0-9_.-]/gi, '_')}.csv`,
          Number(req.body?.partitionCount || 3)
        )
      } finally {
        await fs.rm(directory, { recursive: true, force: true })
      }
      const materializedNode = await db.query(
        `INSERT INTO lineage_nodes
           (kind, operator, dataset_id, job_id, logical_partition_key, metadata_json)
         VALUES ('materialization','MATERIALIZE',$1,$2,$3,$4) RETURNING id`,
        [materializedDataset.datasetId, jobId, `materialization-${materializedDataset.datasetId}`,
          JSON.stringify({ action, lazyPlanId: lazy.id })]
      )
      await db.query(
        `INSERT INTO lineage_edges (parent_id, child_id, edge_type)
         SELECT id, $2, 'materializes' FROM lineage_nodes
         WHERE job_id = $1 AND logical_partition_key = '__job__' ON CONFLICT DO NOTHING`,
        [jobId, materializedNode.rows[0].id]
      )
    }
    await db.query(
      `UPDATE lazy_plans SET status = 'completed', executed_job_id = $2,
       materialized_dataset_id = $3, updated_at = NOW() WHERE id = $1`,
      [lazy.id, jobId, materializedDataset?.datasetId || null]
    )
    res.json({ planId: lazy.id, action, jobId, rowCount: rows.length,
      rows: action === 'collect' ? rows : undefined,
      count: action === 'count' ? rows.length : undefined,
      materializedDataset })
  } catch (error) {
    await db.query(`UPDATE lazy_plans SET status = 'failed', updated_at = NOW() WHERE id = $1`, [req.params.id]).catch(() => {})
    res.status(500).json({ error: error.message })
  }
})

module.exports = router
