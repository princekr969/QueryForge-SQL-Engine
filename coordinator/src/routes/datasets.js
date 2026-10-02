'use strict'

const express  = require('express')
const multer   = require('multer')
const fs       = require('fs')
const fsPromises = require('fs/promises')
const os       = require('os')
const path     = require('path')
const crypto   = require('crypto')
const db       = require('../db')
const { partitionAndStore } = require('../services/partitioner')

const router  = express.Router()

// Uploads land on disk so coordinator memory is independent of dataset size.
const uploadDirectory = path.join(os.tmpdir(), 'queryforge-uploads')
fs.mkdirSync(uploadDirectory, { recursive: true })

const csvFileFilter = (req, file, cb) => {
  if (file.mimetype === 'text/csv' || file.originalname.toLowerCase().endsWith('.csv')) {
    cb(null, true)
  } else {
    cb(new Error('Only CSV files are supported'), false)
  }
}
const upload = multer({
  storage: multer.diskStorage({
    destination: uploadDirectory,
    filename: (_req, _file, cb) => cb(null, `${crypto.randomUUID()}.csv`)
  }),
  limits: { fileSize: 1024 * 1024 * 1024 },  // 1GB
  fileFilter: csvFileFilter
})

// POST /api/datasets/upload
router.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded. Use field name "file".' })
    }

    if (!req.file.originalname.toLowerCase().endsWith('.csv')) {
      return res.status(400).json({ error: 'Only CSV files are supported' })
    }

    const requestedPartitionCount = Number(req.body.partitionCount || 3)
    if (!Number.isSafeInteger(requestedPartitionCount) || requestedPartitionCount < 1 || requestedPartitionCount > 32) {
      return res.status(400).json({ error: 'partitionCount must be an integer from 1 to 32' })
    }

    const { datasetId, snapshotId, contentChecksum, rowCount, schema, partitionCount } = await partitionAndStore(
      req.file.path,
      req.file.originalname,
      requestedPartitionCount
    )

    res.status(201).json({ datasetId, snapshotId, contentChecksum, rowCount, schema, partitionCount })
  } catch (err) {
    console.error('[Route /upload] Error:', err.message)
    res.status(500).json({ error: err.message })
  } finally {
    if (req.file?.path) {
      await fsPromises.unlink(req.file.path).catch(() => {})
    }
  }
})

// GET /api/datasets
router.get('/', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, id AS snapshot_id, name, original_filename, row_count, partition_count,
       schema_json, content_checksum, snapshot_version, generator_config_json, created_at
       FROM datasets ORDER BY created_at DESC`
    )
    res.json(result.rows)
  } catch (err) {
    console.error('[Route GET /datasets] Error:', err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/datasets/:id
router.get('/:id', async (req, res) => {
  try {
    const dsResult = await db.query('SELECT * FROM datasets WHERE id = $1', [req.params.id])
    if (dsResult.rows.length === 0) {
      return res.status(404).json({ error: 'Dataset not found' })
    }

    const partResult = await db.query(
      'SELECT * FROM partitions WHERE dataset_id = $1 ORDER BY partition_index',
      [req.params.id]
    )

    res.json({ dataset: dsResult.rows[0], partitions: partResult.rows })
  } catch (err) {
    console.error('[Route GET /datasets/:id] Error:', err.message)
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/datasets/:id/storage-format — reversible CSV/Parquet routing.
router.patch('/:id/storage-format', async (req, res) => {
  const format = req.body?.format
  if (!['csv', 'parquet'].includes(format)) {
    return res.status(400).json({ error: 'format must be csv or parquet' })
  }

  const client = await db.getClient()
  try {
    await client.query('BEGIN')
    const dataset = await client.query('SELECT * FROM datasets WHERE id = $1 FOR UPDATE', [req.params.id])
    if (dataset.rowCount === 0) {
      await client.query('ROLLBACK')
      return res.status(404).json({ error: 'Dataset not found' })
    }
    const running = await client.query(
      `SELECT 1 FROM jobs WHERE dataset_id = $1 AND status IN ('pending', 'running') LIMIT 1`,
      [req.params.id]
    )
    if (running.rowCount > 0) {
      await client.query('ROLLBACK')
      return res.status(409).json({ error: 'Cannot change storage format while a query is running' })
    }
    if (format === 'parquet') {
      const missing = await client.query(
        'SELECT COUNT(*)::int AS count FROM partitions WHERE dataset_id = $1 AND parquet_path IS NULL',
        [req.params.id]
      )
      if (missing.rows[0].count > 0) {
        await client.query('ROLLBACK')
        return res.status(409).json({ error: 'Dataset does not have a complete Parquet snapshot' })
      }
    }
    await client.query('UPDATE datasets SET storage_format = $2 WHERE id = $1', [req.params.id, format])
    await client.query('COMMIT')
    res.json({ datasetId: req.params.id, storageFormat: format })
  } catch (err) {
    try { await client.query('ROLLBACK') } catch {}
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

module.exports = router
