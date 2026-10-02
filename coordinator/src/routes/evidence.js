'use strict'

const express = require('express')
const fs = require('fs/promises')
const path = require('path')

const router = express.Router()
const candidates = [
  process.env.MILESTONE2_EVIDENCE_PATH,
  '/evidence/milestone2-report.json',
  path.join(__dirname, '../../../benchmarks/artifacts/milestone2-report.json')
].filter(Boolean)

router.get('/milestone2', async (_req, res) => {
  for (const candidate of candidates) {
    try {
      const report = JSON.parse(await fs.readFile(candidate, 'utf8'))
      return res.json(report)
    } catch (error) {
      if (error.code !== 'ENOENT') return res.status(500).json({ error: `Invalid evidence artifact: ${error.message}` })
    }
  }
  res.status(503).json({ error: 'Milestone 2 evidence artifact is unavailable' })
})

module.exports = router
