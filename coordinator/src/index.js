'use strict'

require('dotenv').config()

process.on('uncaughtException', (err) => {
  console.error('[Coordinator] Uncaught exception:', err)
  process.exit(1)
})

process.on('unhandledRejection', (reason) => {
  console.error('[Coordinator] Unhandled rejection:', reason)
  // Do not exit — log and continue
})

const express  = require('express')
const http     = require('http')
const cors     = require('cors')

const { initBuckets }                      = require('./services/partitioner')
const { startCoordinatorGrpcServer,
        heartbeatCheckInterval }           = require('./grpc/coordinatorServer')
const { attachWebSocketServer }            = require('./websocket/wsServer')
const db                                   = require('./db')
const { recoverInterruptedJobs }           = require('./services/jobManager')

const datasetsRouter = require('./routes/datasets')
const queryRouter    = require('./routes/query')
const workersRouter  = require('./routes/workers')
const explainRouter  = require('./routes/explain')
const chaosRouter    = require('./routes/chaos')
const approximateRouter = require('./routes/approximate')
const plansRouter = require('./routes/plans')
const lineageRouter = require('./routes/lineage')
const workloadsRouter = require('./routes/workloads')
const streamsRouter = require('./routes/streams')
const evidenceRouter = require('./routes/evidence')
const { resumeStreams, shutdownStreaming } = require('./services/streamingEngine')

const PORT = parseInt(process.env.PORT || '3000', 10)
const coordinatorStartedAt = new Date()

let grpcServer  = null
let httpServer  = null

async function main () {
  console.log('[Coordinator] Applying database migrations...')
  await db.runMigrations()

  console.log('[Coordinator] Initialising MinIO buckets...')
  await initBuckets()

  console.log('[Coordinator] Starting gRPC server...')
  grpcServer = await startCoordinatorGrpcServer()

  const app = express()
  app.use(cors())
  app.use(express.json())

  app.use('/api/datasets', datasetsRouter)
  app.use('/api/query',    queryRouter)
  app.use('/api/workers',  workersRouter)
  app.use('/api/explain',  explainRouter)
  app.use('/api/chaos',    chaosRouter)
  app.use('/api/approximate', approximateRouter)
  app.use('/api/plans', plansRouter)
  app.use('/api/lineage', lineageRouter)
  app.use('/api/workloads', workloadsRouter)
  app.use('/api/streams', streamsRouter)
  app.use('/api/evidence', evidenceRouter)
  app.get('/api/health',   (req, res) => res.json({ status: 'ok', ts: Date.now() }))

  app.use((req, res) => res.status(404).json({ error: `Not found: ${req.method} ${req.path}` }))
  app.use((err, req, res, _next) => {
    console.error('[Coordinator] Express error:', err.message)
    res.status(500).json({ error: err.message })
  })

  httpServer = http.createServer(app)
  attachWebSocketServer(httpServer)

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`[Coordinator] HTTP + WebSocket listening on port ${PORT}`)
  })

  setTimeout(() => {
    recoverInterruptedJobs(coordinatorStartedAt)
      .then(count => { if (count > 0) console.log(`[Recovery] Replayed ${count} interrupted job(s)`) })
      .catch(error => console.error('[Recovery] Failed to inspect interrupted jobs:', error.message))
  }, Number(process.env.RECOVERY_DELAY_MS || 8000)).unref()
  setTimeout(() => resumeStreams().catch(error => console.warn('[Streaming] Resume failed:', error.message)), 2000).unref()
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown (signal) {
  console.log(`[Coordinator] ${signal} received — shutting down gracefully`)

  if (heartbeatCheckInterval) clearInterval(heartbeatCheckInterval)
  await shutdownStreaming()

  if (httpServer) {
    await new Promise(resolve => httpServer.close(resolve))
  }

  if (grpcServer) {
    await new Promise(resolve => grpcServer.tryShutdown(resolve))
  }

  await db.end()
  console.log('[Coordinator] Shutdown complete')
  process.exit(0)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT',  () => shutdown('SIGINT'))

main().catch(err => {
  console.error('[Coordinator] Fatal startup error:', err)
  process.exit(1)
})
