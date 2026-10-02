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

// Cache of worker stubs keyed by "host:port"
const stubCache = new Map()

/**
 * Get or create a gRPC stub for a worker address.
 * Uses WAIT_FOR_READY so transient connection issues don't immediately fail.
 */
function getWorkerStub (address) {
  if (!stubCache.has(address)) {
    const stub = new proto.WorkerService(
      address,
      grpc.credentials.createInsecure(),
      {
        'grpc.wait_for_ready': 1,
        'grpc.max_send_message_length': 16 * 1024 * 1024,
        'grpc.max_receive_message_length': 16 * 1024 * 1024
      }
    )
    stubCache.set(address, stub)
  }
  return stubCache.get(address)
}

function processStreamBatchOnWorker (workerAddress, request, deadlineMs = 10000) {
  return new Promise((resolve, reject) => {
    const stub = getWorkerStub(workerAddress)
    stub.ProcessStreamBatch(request, { deadline: Date.now() + deadlineMs }, (error, response) => {
      if (error) return reject(error)
      if (response.batch_id !== request.batch_id || Number(response.event_count) !== JSON.parse(request.events_json).length) {
        return reject(new Error('Worker returned an incomplete or mismatched stream micro-batch'))
      }
      try { resolve(JSON.parse(response.records_json)) } catch { reject(new Error('Worker returned invalid stream records')) }
    })
  })
}

/**
 * Invalidate a cached stub (call when a worker is known to have restarted).
 */
function invalidateWorkerStub (address) {
  const stub = stubCache.get(address)
  if (stub) {
    try { stub.close() } catch {}
    stubCache.delete(address)
  }
}

/**
 * Execute a task on a remote worker via server-side streaming.
 * Resolves with array of all PartialResult messages received.
 */
function executeTaskOnWorker (workerAddress, taskRequest, deadlineMs = 30_000, signal = null) {
  return new Promise((resolve, reject) => {
    const stub    = getWorkerStub(workerAddress)
    const results = []

    const deadline = new Date(Date.now() + deadlineMs)
    const call     = stub.ExecuteTask(taskRequest, { deadline })
    const abort = () => call.cancel()
    if (signal?.aborted) abort()
    else signal?.addEventListener('abort', abort, { once: true })

    let settled = false
    const settle = (fn, value) => {
      if (settled) return
      settled = true
      signal?.removeEventListener('abort', abort)
      fn(value)
    }

    call.on('data', (partialResult) => {
      if (partialResult.task_id !== taskRequest.task_id) {
        call.cancel()
        settle(reject, new Error(`Worker returned result for unexpected task ${partialResult.task_id}`))
        return
      }
      results.push(partialResult)
    })
    call.on('end', () => {
      const terminalResults = results.filter(result => result.is_complete)
      if (terminalResults.length !== 1 || results[results.length - 1]?.is_complete !== true) {
        settle(reject, new Error(`Worker stream for task ${taskRequest.task_id} ended without one terminal result`))
        return
      }
      settle(resolve, results)
    })
    call.on('error', (err) => settle(reject, err))
  })
}

/**
 * Ping a worker — returns true if alive within 3 seconds.
 */
function pingWorker (workerAddress) {
  return new Promise((resolve) => {
    const stub = getWorkerStub(workerAddress)
    stub.Ping({}, { deadline: Date.now() + 3000 }, (err, response) => {
      resolve(!err && response && response.alive === true)
    })
  })
}

function inspectWorker (workerAddress) {
  return new Promise((resolve) => {
    const stub = getWorkerStub(workerAddress)
    stub.Ping({}, { deadline: Date.now() + 3000 }, (err, response) => {
      if (err || !response?.alive) return resolve({ alive: false, cache: null })
      try {
        resolve({ alive: true, cache: JSON.parse(response.cache_stats_json || '{}') })
      } catch {
        resolve({ alive: true, cache: null })
      }
    })
  })
}

module.exports = { executeTaskOnWorker, processStreamBatchOnWorker, pingWorker, inspectWorker, getWorkerStub, invalidateWorkerStub }
