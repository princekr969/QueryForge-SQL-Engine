'use strict'

const grpc        = require('@grpc/grpc-js')
const protoLoader = require('@grpc/proto-loader')
const path        = require('path')
const { executeTask } = require('../services/taskExecutor')
const { snapshot: cacheSnapshot } = require('../services/cacheManager')
const { processStreamBatch } = require('../services/streamBatchExecutor')
const { incrementActiveTasks, decrementActiveTasks } = require('../heartbeat')

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

/**
 * ExecuteTask — server-side streaming RPC.
 * A successful stream always ends with a terminal PartialResult. Failures are
 * surfaced as gRPC errors so the coordinator can retry the partition safely.
 */
async function executeTaskRpc (call) {
  const request = call.request
  console.log(`[Worker] ExecuteTask — task ${request.task_id}, partition: ${request.partition_path}`)

  try {
    await executeTask(request, (partialResult) => {
      call.write(partialResult)
    })
    console.log(`[Worker] ExecuteTask complete — task ${request.task_id}`)
    call.end()
  } catch (err) {
    console.error(`[Worker] ExecuteTask error — task ${request.task_id}:`, err.message)
    const rpcError = new Error(`Task ${request.task_id} failed: ${err.message}`)
    rpcError.code = grpc.status.INTERNAL
    try { call.destroy(rpcError) } catch {}
  }
}

function ping (call, callback) {
  callback(null, { alive: true, cache_stats_json: JSON.stringify(cacheSnapshot()) })
}

async function processStreamBatchRpc (call, callback) {
  incrementActiveTasks()
  try {
    const config = JSON.parse(call.request.config_json)
    if (config.workerDelayMs) await new Promise(resolve => setTimeout(resolve, config.workerDelayMs))
    callback(null, processStreamBatch(call.request))
  } catch (error) {
    callback({ code: grpc.status.INVALID_ARGUMENT, message: error.message })
  } finally {
    decrementActiveTasks()
  }
}

function startWorkerGrpcServer (port) {
  const server = new grpc.Server()

  server.addService(proto.WorkerService.service, {
    ExecuteTask: executeTaskRpc,
    ProcessStreamBatch: processStreamBatchRpc,
    Ping:        ping
  })

  return new Promise((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, boundPort) => {
        if (err) return reject(err)
        console.log(`[Worker] gRPC WorkerService listening on port ${boundPort}`)
        resolve(server)
      }
    )
  })
}

module.exports = { startWorkerGrpcServer }
