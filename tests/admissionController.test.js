'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { AdmissionController } = require('../coordinator/src/services/admissionController')

describe('priority admission control', () => {
  it('runs no more than the configured concurrency and drains high priority first', async () => {
    const controller = new AdmissionController({ maxConcurrent: 1, maxQueued: 3 })
    const order = []
    let release
    const blocker = new Promise(resolve => { release = resolve })
    const first = controller.admit({ jobId: 'first', priority: 0, run: async () => { order.push('first'); await blocker } })
    const low = controller.admit({ jobId: 'low', priority: -1, run: async () => { order.push('low') } })
    const high = controller.admit({ jobId: 'high', priority: 10, run: async () => { order.push('high') } })
    await new Promise(resolve => setImmediate(resolve))
    assert.deepEqual(order, ['first'])
    assert.equal(controller.snapshot().active, 1)
    release()
    await Promise.all([first.promise, low.promise, high.promise])
    assert.deepEqual(order, ['first', 'high', 'low'])
  })

  it('rejects work once the waiting queue is full', async () => {
    const controller = new AdmissionController({ maxConcurrent: 1, maxQueued: 1 })
    let release
    const blocker = new Promise(resolve => { release = resolve })
    const running = controller.admit({ jobId: 'running', run: () => blocker })
    controller.admit({ jobId: 'queued', run: async () => {} })
    assert.throws(() => controller.admit({ jobId: 'overflow', run: async () => {} }), /queue is full/)
    release()
    await running.promise
  })
})
