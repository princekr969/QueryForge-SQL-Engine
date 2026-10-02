'use strict'

const express = require('express')
const db = require('../db')
const { runApproximateQuery } = require('../services/approximateQuery')

const router = express.Router()

router.post('/', async (req, res) => {
  try {
    res.status(201).json(await runApproximateQuery(req.body || {}))
  } catch (error) {
    const clientError = /required|must be|Unknown column|not found|Parquet snapshot/.test(error.message)
    res.status(clientError ? 400 : 503).json({ error: error.message })
  }
})

router.get('/runs', async (req, res) => {
  try {
    const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 50))
    const values = []
    let where = ''
    if (req.query.datasetId) {
      values.push(req.query.datasetId)
      where = 'WHERE dataset_id = $1'
    }
    values.push(limit)
    const result = await db.query(
      `SELECT * FROM approximate_runs ${where} ORDER BY created_at DESC LIMIT $${values.length}`, values
    )
    res.json(result.rows)
  } catch (error) {
    res.status(500).json({ error: error.message })
  }
})

module.exports = router
