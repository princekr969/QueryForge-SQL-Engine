'use strict'

const grpc        = require('@grpc/grpc-js')
const protoLoader = require('@grpc/proto-loader')
const path        = require('path')

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

let coordinatorStub = null

function getStub () {
  if (!coordinatorStub) {
    const address = process.env.COORDINATOR_ADDRESS || 'coordinator:50050'
    coordinatorStub = new proto.CoordinatorService(
      address,
      grpc.credentials.createInsecure()
    )
  }
  return coordinatorStub
}

/**
 * Register this worker with the coordinator.
 * Retries up to `retries` times with `delayMs` between each attempt.
 */
async function registerWithCoordinator (workerId, address, port, retries = 10, delayMs = 3000) {
  for (let i = 0; i < retries; i++) {
    try {
      await new Promise((resolve, reject) => {
        getStub().Register(
          { worker_id: workerId, address, port, capabilities: ['scan', 'aggregate', 'join', 'shuffle', 'sketch', 'stream'] },
          { deadline: Date.now() + 5000 },
          (err, response) => {
            if (err) return reject(err)
            if (!response.success) return reject(new Error('Coordinator rejected registration'))
            resolve()
          }
        )
      })
      console.log(`[Worker] Registered with coordinator as ${workerId}`)
      return
    } catch (err) {
      console.log(`[Worker] Registration attempt ${i + 1}/${retries} failed: ${err.message} — retrying in ${delayMs}ms...`)
      await new Promise(res => setTimeout(res, delayMs))
    }
  }
  throw new Error(`[Worker] Could not register after ${retries} attempts`)
}

function sendHeartbeat (workerId, activeTasks) {
  return new Promise((resolve, reject) => {
    getStub().Heartbeat(
      { worker_id: workerId, status: 'alive', active_tasks: activeTasks },
      { deadline: Date.now() + 3000 },
      (err) => { if (err) return reject(err); resolve() }
    )
  })
}

module.exports = { registerWithCoordinator, sendHeartbeat }
