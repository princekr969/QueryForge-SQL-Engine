'use strict'

const express = require('express')
const crypto = require('crypto')
const db = require('../db')
const { executeQuery } = require('../services/jobManager')

const router = express.Router()

router.post('/invalidate', async (req, res) => {
  try {
    const { jobId, logicalPartitionKey, reason = 'derived partition deleted' } = req.body || {}
    if (!jobId || !logicalPartitionKey) return res.status(400).json({ error: 'jobId and logicalPartitionKey are required' })
    const invalidated = await db.query(
      `UPDATE lineage_nodes SET status = 'missing',
       metadata_json = metadata_json || $3::jsonb
       WHERE job_id = $1 AND logical_partition_key = $2 AND kind = 'partition'
       RETURNING id, job_id, logical_partition_key, status`,
      [jobId, logicalPartitionKey, JSON.stringify({ invalidatedAt: new Date().toISOString(), invalidationReason: String(reason) })]
    )
    if (invalidated.rowCount === 0) return res.status(404).json({ error: 'Derived lineage partition not found' })
    res.json(invalidated.rows[0])
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.get('/', async (req, res) => {
  try {
    let nodes
    if (req.query.datasetId) {
      nodes = await db.query(
        `WITH RECURSIVE graph(id) AS (
           SELECT id FROM lineage_nodes WHERE dataset_id = $1
           UNION
           SELECT edge.child_id FROM lineage_edges edge JOIN graph ON edge.parent_id = graph.id
         ) SELECT node.* FROM lineage_nodes node JOIN graph USING (id) ORDER BY node.created_at`,
        [req.query.datasetId]
      )
    } else if (req.query.jobId) {
      nodes = await db.query('SELECT * FROM lineage_nodes WHERE job_id = $1 ORDER BY created_at', [req.query.jobId])
    } else nodes = await db.query('SELECT * FROM lineage_nodes ORDER BY created_at LIMIT 1000')
    const ids = nodes.rows.map(item => item.id)
    const edges = ids.length
      ? await db.query('SELECT * FROM lineage_edges WHERE parent_id = ANY($1::uuid[]) OR child_id = ANY($1::uuid[])', [ids])
      : { rows: [] }
    res.json({ nodes: nodes.rows, edges: edges.rows })
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.post('/recover', async (req, res) => {
  const { jobId, logicalPartitionKey } = req.body || {}
  try {
    if (!jobId || !logicalPartitionKey) return res.status(400).json({ error: 'jobId and logicalPartitionKey are required' })
    const source = await db.query('SELECT * FROM jobs WHERE id = $1 AND status = $2', [jobId, 'completed'])
    if (source.rowCount === 0) return res.status(404).json({ error: 'Completed source job not found' })
    const node = await db.query(
      `UPDATE lineage_nodes SET status = 'rebuilding'
       WHERE job_id = $1 AND logical_partition_key = $2
       AND kind = 'partition' AND status IN ('missing','failed') RETURNING id`, [jobId, logicalPartitionKey]
    )
    if (node.rowCount === 0) return res.status(409).json({ error: 'Lineage partition must be missing or failed before recovery' })
    const ancestor = await db.query(
      `WITH RECURSIVE ancestors(id, depth, replay_path) AS (
         SELECT $1::uuid, 0, ARRAY[$1::uuid]
         UNION ALL
         SELECT edge.parent_id, ancestors.depth + 1, ancestors.replay_path || edge.parent_id
         FROM lineage_edges edge JOIN ancestors ON edge.child_id = ancestors.id
         WHERE NOT edge.parent_id = ANY(ancestors.replay_path)
       )
       SELECT node.id, node.kind, node.operator, node.dataset_id, node.logical_partition_key,
              ancestors.depth, ancestors.replay_path
       FROM ancestors JOIN lineage_nodes node ON node.id = ancestors.id
       WHERE ancestors.depth > 0 AND node.status = 'available'
         AND node.kind IN ('source','materialization','partition')
       ORDER BY ancestors.depth ASC,
         CASE node.kind WHEN 'materialization' THEN 0 WHEN 'partition' THEN 1 ELSE 2 END
       LIMIT 1`, [node.rows[0].id]
    )
    if (ancestor.rowCount === 0) throw new Error('No valid cached or materialized lineage ancestor is available')
    const recoveryJobId = crypto.randomUUID()
    const context = source.rows[0].query_context_json || {}
    await executeQuery(source.rows[0].sql_query, source.rows[0].dataset_id, recoveryJobId,
      context.datasetIds || {}, { ...(source.rows[0].resource_budget_json || {}), recoverLogicalPartition: logicalPartitionKey })
    const [originalTask, recoveryTask] = await Promise.all([
      db.query('SELECT partial_result_checksum FROM tasks WHERE job_id=$1 AND logical_partition_key=$2 AND is_winner=TRUE', [jobId, logicalPartitionKey]),
      db.query('SELECT partial_result_checksum FROM tasks WHERE job_id=$1 AND is_winner=TRUE', [recoveryJobId])
    ])
    const partitionChecksumMatch = originalTask.rows[0]?.partial_result_checksum === recoveryTask.rows[0]?.partial_result_checksum
    if (!partitionChecksumMatch) throw new Error('Recovered partition checksum did not match its lineage ancestor')
    await db.query(
      `UPDATE lineage_nodes SET status = 'available',
       metadata_json = metadata_json || $3::jsonb WHERE job_id = $1 AND logical_partition_key = $2`,
      [jobId, logicalPartitionKey, JSON.stringify({
        recoveredByJobId: recoveryJobId,
        recoveryAncestorId: ancestor.rows[0].id,
        recoveryPath: ancestor.rows[0].replay_path
      })]
    )
    res.json({ sourceJobId: jobId, logicalPartitionKey, recoveryJobId, recomputedPartitions: 1,
      partitionChecksumMatch, preservedResultChecksum: source.rows[0].result_checksum,
      ancestor: {
        id: ancestor.rows[0].id, kind: ancestor.rows[0].kind,
        operator: ancestor.rows[0].operator, datasetId: ancestor.rows[0].dataset_id
      },
      replayPath: ancestor.rows[0].replay_path })
  } catch (error) {
    if (jobId && logicalPartitionKey) {
      await db.query(
        `UPDATE lineage_nodes SET status='missing'
         WHERE job_id=$1 AND logical_partition_key=$2 AND status='rebuilding'`,
        [jobId, logicalPartitionKey]
      ).catch(() => {})
    }
    res.status(500).json({ error: error.message })
  }
})

module.exports = router
