'use strict'

const express          = require('express')
const db               = require('../db')
const { workerRegistry } = require('../services/workerRegistry')
const { inspectWorker } = require('../grpc/workerClient')

const router = express.Router()

// GET /api/workers — list all registered workers with live status
router.get('/', async (req, res) => {
  try {
    // Merge DB record with live in-memory status
    const dbResult = await db.query('SELECT * FROM workers ORDER BY registered_at')

    const visibleRows = req.query.includeHistory === 'true'
      ? dbResult.rows
      : dbResult.rows.filter(worker => workerRegistry.has(worker.id))
    const workers = await Promise.all(visibleRows.map(async w => {
      const live = workerRegistry.get(w.id)
      const address = live ? `${live.address}:${live.port}` : null
      const inspection = address ? await inspectWorker(address) : { alive: false, cache: null }
      return {
        ...w,
        liveStatus:    live?.status        || 'unknown',
        activeTasks:   live?.activeTasks   || 0,
        lastHeartbeat: live?.lastHeartbeat || null,
        grpcAlive: inspection.alive,
        cache: inspection.cache
      }
    }))

    res.json(workers)
  } catch (err) {
    console.error('[Route GET /workers] Error:', err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/health — coordinator health check
router.get('/health', (req, res) => {
  const activeCount = [...workerRegistry.values()].filter(w => w.status === 'active').length
  res.json({
    status: 'ok',
    activeWorkers: activeCount,
    timestamp: new Date().toISOString()
  })
})

// GET /api/workers/discover — service discovery endpoint
// Returns only active workers available for task assignment.
// Used by the job manager to resolve healthy worker addresses at query time,
// enabling dynamic service discovery without hardcoded worker addresses.
router.get('/discover', (req, res) => {
  const activeWorkers = [...workerRegistry.values()]
    .filter(w => w.status === 'active')
    .map(w => ({
      workerId:      w.workerId,
      address:       w.address,
      port:          w.port,
      activeTasks:   w.activeTasks,
      lastHeartbeat: w.lastHeartbeat,
      status:        w.status
    }))

  res.json({
    count:   activeWorkers.length,
    workers: activeWorkers,
    timestamp: new Date().toISOString()
  })
})

// POST /api/workers/register — REST-based service registration
// Allows workers to announce themselves to the coordinator over HTTP
// in addition to the gRPC registration channel.
router.post('/register', async (req, res) => {
  const { worker_id, address, port } = req.body

  if (!worker_id || !address || !port) {
    return res.status(400).json({ error: 'worker_id, address, and port are required' })
  }

  workerRegistry.set(worker_id, {
    workerId:      worker_id,
    address,
    port,
    lastHeartbeat: Date.now(),
    status:        'active',
    activeTasks:   0
  })

  try {
    await db.query(
      `INSERT INTO workers (id, address, port, status, last_heartbeat, registered_at)
       VALUES ($1, $2, $3, 'active', NOW(), NOW())
       ON CONFLICT (id) DO UPDATE
         SET address = EXCLUDED.address, port = EXCLUDED.port,
             status = 'active', last_heartbeat = NOW()`,
      [worker_id, address, port]
    )
  } catch (err) {
    console.error('[Route POST /workers/register] DB error:', err.message)
  }

  console.log(`[ServiceDiscovery] Worker registered via REST: ${worker_id} @ ${address}:${port}`)
  res.status(201).json({ success: true, worker_id, address, port })
})

module.exports = router
