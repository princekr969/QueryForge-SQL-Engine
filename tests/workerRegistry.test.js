'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { compareWorkerSchedulingOrder } = require('../coordinator/src/services/workerRegistry')

test('fixed course workers are scheduled before hostname-derived elastic workers', () => {
  const workers = [
    { workerId: 'worker-20e3a2607cd0' },
    { workerId: 'worker-03ac5da4345d' },
    { workerId: 'worker-2' },
    { workerId: 'worker-3' },
    { workerId: 'worker-1' }
  ]

  assert.deepEqual(
    workers.sort(compareWorkerSchedulingOrder).map(worker => worker.workerId),
    ['worker-1', 'worker-2', 'worker-3', 'worker-03ac5da4345d', 'worker-20e3a2607cd0']
  )
})
