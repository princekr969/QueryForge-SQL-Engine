'use strict'

const express = require('express')
const fs = require('fs/promises')
const os = require('os')
const path = require('path')
const { stringify } = require('csv-stringify/sync')
const db = require('../db')
const { partitionAndStore } = require('../services/partitioner')
const {
  parseStandingStatement, normalizeStreamConfig, startStream, stopStream,
  publishEvents, snapshot, stateResult
} = require('../services/streamingEngine')

const router = express.Router()

router.post('/register', async (req, res) => {
  try {
    const config = req.body?.statement
      ? parseStandingStatement(req.body.statement)
      : normalizeStreamConfig(req.body || {})
    const inserted = await db.query(
      `INSERT INTO stream_queries (name,source_topic,statement,config_json,status)
       VALUES ($1,$2,$3,$4,'active') RETURNING *`,
      [config.name, config.topic, req.body?.statement || null, JSON.stringify(config)]
    )
    await db.query(
      `INSERT INTO lineage_nodes
         (kind,operator,logical_partition_key,metadata_json,status)
       VALUES ('transformation','STREAM_QUERY',$1,$2,'available')`,
      [`stream-${inserted.rows[0].id}`, JSON.stringify({
        streamQueryId: inserted.rows[0].id, topic: config.topic, config
      })]
    )
    try {
      await startStream(inserted.rows[0])
    } catch (error) {
      await db.query("UPDATE stream_queries SET status='paused' WHERE id=$1", [inserted.rows[0].id])
      await db.query(
        `UPDATE lineage_nodes SET status='failed'
         WHERE operator='STREAM_QUERY' AND metadata_json->>'streamQueryId'=$1`,
        [inserted.rows[0].id]
      )
      throw new Error(`Kafka source unavailable: ${error.message}`)
    }
    res.status(201).json({ ...inserted.rows[0], configuredHllError: config.distinctColumn ? 1.04 / Math.sqrt(2 ** config.hllPrecision) : null })
  } catch (error) {
    const status = /unavailable/.test(error.message) ? 503 : 400
    res.status(status).json({ error: error.message })
  }
})

router.post('/unregister', async (req, res) => {
  try {
    const statement = String(req.body?.statement || '')
    const match = statement.match(/^\s*UNREGISTER\s+QUERY\s+([a-zA-Z_][\w]*)\s*;?\s*$/i)
    const name = match?.[1] || req.body?.name
    if (!name || !/^[a-zA-Z_][\w]*$/.test(name)) {
      return res.status(400).json({ error: 'Expected UNREGISTER QUERY <name>' })
    }
    const query = await db.query('SELECT id FROM stream_queries WHERE name=$1', [name])
    if (query.rowCount === 0) return res.status(404).json({ error: 'Stream query not found' })
    await stopStream(query.rows[0].id)
    await db.query("UPDATE stream_queries SET status='stopped',updated_at=NOW() WHERE id=$1", [query.rows[0].id])
    await db.query(
      `UPDATE lineage_nodes SET status='stopped'
       WHERE operator='STREAM_QUERY' AND metadata_json->>'streamQueryId'=$1`, [query.rows[0].id]
    )
    res.json({ id: query.rows[0].id, name, status: 'stopped' })
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.post('/:id/start', async (req, res) => {
  try {
    const query = await db.query('SELECT * FROM stream_queries WHERE id=$1', [req.params.id])
    if (query.rowCount === 0) return res.status(404).json({ error: 'Stream query not found' })
    await startStream(query.rows[0])
    await db.query("UPDATE stream_queries SET status='active',updated_at=NOW() WHERE id=$1", [req.params.id])
    res.json({ id: req.params.id, status: 'active' })
  } catch (error) { res.status(503).json({ error: error.message }) }
})

router.delete('/:id', async (req, res) => {
  try {
    await stopStream(req.params.id)
    const updated = await db.query("UPDATE stream_queries SET status='stopped',updated_at=NOW() WHERE id=$1 RETURNING id,name,status", [req.params.id])
    if (updated.rowCount === 0) return res.status(404).json({ error: 'Stream query not found' })
    res.json(updated.rows[0])
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.get('/:id', async (req, res) => {
  try {
    const value = await snapshot(req.params.id)
    if (!value) return res.status(404).json({ error: 'Stream query not found' })
    res.json(value)
  } catch (error) { res.status(500).json({ error: error.message }) }
})

router.post('/publish/:topic', async (req, res) => {
  try {
    const events = Array.isArray(req.body?.events) ? req.body.events : []
    if (events.length < 1 || events.length > 10000) return res.status(400).json({ error: 'events must contain 1-10000 records' })
    const count = await publishEvents(req.params.topic, events)
    res.status(202).json({ topic: req.params.topic, published: count })
  } catch (error) { res.status(503).json({ error: error.message }) }
})

router.post('/:id/materialize', async (req, res) => {
  try {
    const current = await snapshot(req.params.id)
    if (!current) return res.status(404).json({ error: 'Stream query not found' })
    if (current.windows.length === 0) return res.status(409).json({ error: 'Stream query has no window state' })
    const rows = current.windows.map(window => ({
      window_start: new Date(window.window_start).toISOString(),
      window_end: new Date(window.window_end).toISOString(), group_key: window.group_key,
      event_count: Number(window.event_count), status: window.status,
      distinct_users: window.result.distinctUsers,
      hll_error_bound: window.result.distinctConfiguredError,
      revenue: window.result.revenue,
      top_items: JSON.stringify(window.result.topItems)
    }))
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'queryforge-stream-materialize-'))
    try {
      const outputPath = path.join(directory, 'windows.csv')
      await fs.writeFile(outputPath, stringify(rows, { header: true }))
      const dataset = await partitionAndStore(outputPath,
        `${String(req.body?.name || `${current.query.name}_windows`).replace(/[^a-z0-9_.-]/gi, '_')}.csv`,
        Number(req.body?.partitionCount || 3))
      await db.query('UPDATE stream_queries SET materialized_dataset_id=$2,updated_at=NOW() WHERE id=$1', [req.params.id, dataset.datasetId])
      const node = await db.query(
        `INSERT INTO lineage_nodes
           (kind,operator,dataset_id,logical_partition_key,metadata_json)
         VALUES ('materialization','STREAM_MATERIALIZE',$1,$2,$3) RETURNING id`,
        [dataset.datasetId, `stream-${req.params.id}`, JSON.stringify({
          streamQueryId: req.params.id, watermark: current.query.watermark,
          outputEpoch: current.outputEpoch, sourceOffsets: current.offsets
        })]
      )
      const standingQuery = await db.query(
        `SELECT id FROM lineage_nodes WHERE operator='STREAM_QUERY'
         AND metadata_json->>'streamQueryId'=$1 ORDER BY created_at DESC LIMIT 1`, [req.params.id]
      )
      if (standingQuery.rowCount) await db.query(
        `INSERT INTO lineage_edges (parent_id,child_id,edge_type)
         VALUES ($1,$2,'materializes_epoch') ON CONFLICT DO NOTHING`,
        [standingQuery.rows[0].id, node.rows[0].id]
      )
      const source = await db.query(
        "SELECT id FROM lineage_nodes WHERE dataset_id=$1 AND kind='source' LIMIT 1", [dataset.datasetId]
      )
      if (source.rowCount) await db.query(
        `INSERT INTO lineage_edges (parent_id,child_id,edge_type) VALUES ($1,$2,'stream_snapshot') ON CONFLICT DO NOTHING`,
        [node.rows[0].id, source.rows[0].id]
      )
      res.json({ streamQueryId: req.params.id, windows: rows.length, outputEpoch: current.outputEpoch, dataset })
    } finally { await fs.rm(directory, { recursive: true, force: true }) }
  } catch (error) { res.status(500).json({ error: error.message }) }
})

module.exports = router
