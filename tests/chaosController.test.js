'use strict'

const assert = require('node:assert/strict')
const { describe, it, afterEach } = require('node:test')
const chaos = require('../coordinator/src/services/chaosController')

describe('deterministic chaos rules', () => {
  afterEach(() => chaos.clear())

  it('targets a worker and partition for an exact occurrence count', () => {
    chaos.configure({ mode: 'skew', targetWorker: 'worker-2', partitionIndex: 1, delayMs: 500, occurrences: 2 })
    assert.equal(chaos.consume({ workerId: 'worker-1', partitionIndex: 1 }), null)
    assert.equal(chaos.consume({ workerId: 'worker-2', partitionIndex: 0 }), null)
    assert.equal(chaos.consume({ workerId: 'worker-2', partitionIndex: 1 }).mode, 'skew')
    assert.equal(chaos.consume({ workerId: 'worker-2', partitionIndex: 1 }).mode, 'skew')
    assert.equal(chaos.snapshot(), null)
  })

  it('rejects unknown and unbounded rules', () => {
    assert.throws(() => chaos.configure({ mode: 'fire' }), /mode must be/)
    assert.throws(() => chaos.configure({ mode: 'delay', delayMs: 50000 }), /delayMs/)
  })
})
