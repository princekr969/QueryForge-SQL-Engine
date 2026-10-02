'use strict'

const grpc        = require('@grpc/grpc-js')
const protoLoader = require('@grpc/proto-loader')
const path        = require('path')
const db          = require('../db')
const { workerRegistry } = require('../services/workerRegistry')

// Proto path: works both in Docker (/proto) and locally (../../proto)
const PROTO_PATH = process.env.PROTO_PATH ||
  (require('fs').existsSync('/proto/dataforge.proto')
    ? '/proto/dataforge.proto'
    : path.join(__dirname, '../../../proto/dataforge.proto'))

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs:    String,
  enums:    String,
  defaults: true,
  oneofs:   true
})

const proto = grpc.loadPackageDefinition(packageDef).dataforge

const HEARTBEAT_TIMEOUT_MS = 15_000

// ── Heartbeat monitor ─────────────────────────────────────────────────────────
const heartbeatCheckInterval = setInterval(async () => {
  const now = Date.now()
  for (const [id, worker] of workerRegistry.entries()) {
    if (worker.status === 'active' && now - worker.lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
      console.warn(`[Coordinator] Worker ${id} missed heartbeat — marking dead`)
      worker.status = 'dead'
      try {
        await db.query(`UPDATE workers SET status = 'dead' WHERE id = $1`, [id])
      } catch (err) {
        console.error('[Coordinator] Failed to update worker status in DB:', err.message)
      }
    }
  }
}, 5_000)

// ── RPC implementations ───────────────────────────────────────────────────────

async function register (call, callback) {
  const { worker_id, address, port, capabilities = [] } = call.request
  console.log(`[Coordinator] Worker registered: ${worker_id} @ ${address}:${port}`)

  workerRegistry.set(worker_id, {
    workerId:      worker_id,
    address,
    port,
    lastHeartbeat: Date.now(),
    status:        'active',
    activeTasks:   0,
    capabilities
  })

  try {
    await db.query(
      `INSERT INTO workers (id, address, port, status, last_heartbeat, registered_at)
       VALUES ($1, $2, $3, 'active', NOW(), NOW())
       ON CONFLICT (id) DO UPDATE
         SET address = EXCLUDED.address, port = EXCLUDED.port,
             status = 'active', last_heartbeat = NOW(), registered_at = NOW()`,
      [worker_id, address, port]
    )
  } catch (err) {
    console.error('[Coordinator] Failed to persist worker registration:', err.message)
  }

  callback(null, { success: true })
}

async function heartbeat (call, callback) {
  const { worker_id, active_tasks } = call.request
  const worker = workerRegistry.get(worker_id)

  if (worker) {
    worker.lastHeartbeat = Date.now()
    worker.status        = 'active'
    worker.activeTasks   = active_tasks
  } else {
    let persisted = null
    try {
      const result = await db.query('SELECT address, port FROM workers WHERE id = $1', [worker_id])
      persisted = result.rows[0] || null
    } catch {}
    workerRegistry.set(worker_id, {
      workerId:      worker_id,
      address:       persisted?.address || worker_id,
      port:          persisted?.port || parseInt(process.env.WORKER_PORT || '50051', 10),
      lastHeartbeat: Date.now(),
      status:        'active',
      activeTasks:   active_tasks,
      capabilities: worker_id.includes('rust')
        ? ['scan', 'aggregate', 'join', 'stream']
        : ['scan', 'aggregate', 'join', 'shuffle', 'sketch', 'stream']
    })
  }

  try {
    await db.query(
      `UPDATE workers SET status = 'active', last_heartbeat = NOW() WHERE id = $1`,
      [worker_id]
    )
  } catch (err) {
    console.error('[Coordinator] Failed to update heartbeat in DB:', err.message)
  }

  callback(null, { acknowledged: true })
}

// ── Start gRPC server ─────────────────────────────────────────────────────────

function startCoordinatorGrpcServer () {
  const server = new grpc.Server()

  server.addService(proto.CoordinatorService.service, {
    Register:  register,
    Heartbeat: heartbeat
  })

  return new Promise((resolve, reject) => {
    server.bindAsync(
      '0.0.0.0:50050',
      grpc.ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) return reject(err)
        console.log(`[Coordinator] gRPC CoordinatorService listening on port ${port}`)
        resolve(server)
      }
    )
  })
}

module.exports = { startCoordinatorGrpcServer, workerRegistry, proto, heartbeatCheckInterval }
