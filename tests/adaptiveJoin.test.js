'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { chooseJoinStrategy, detectHotBuckets } = require('../coordinator/src/services/adaptiveJoin')

describe('adaptive join cost model', () => {
  const options = { localThreshold: 1000, broadcastThreshold: 5000 }

  it('coalesces two tiny inputs locally', () => {
    assert.equal(chooseJoinStrategy(400, 500, options).strategy, 'local')
  })

  it('broadcasts a sufficiently asymmetric build side', () => {
    assert.equal(chooseJoinStrategy(2000, 10000, options).strategy, 'broadcast')
  })

  it('hash-shuffles balanced large inputs', () => {
    assert.equal(chooseJoinStrategy(10000, 12000, options).strategy, 'hash_shuffle')
  })

  it('detects a distributed hot bucket without flagging balanced buckets', () => {
    const stats = [
      { rows: 90, sourceRows: [30, 30, 30] },
      { rows: 10, sourceRows: [4, 3, 3] },
      { rows: 10, sourceRows: [3, 4, 3] },
      { rows: 10, sourceRows: [3, 3, 4] }
    ]
    assert.deepEqual(detectHotBuckets(stats, 1.5).map(item => item.bucket), [0])
    assert.deepEqual(detectHotBuckets(stats.map(() => ({ rows: 30, sourceRows: [10, 10, 10] })), 1.5), [])
  })

  it('does not split a hot bucket that exists in only one source partition', () => {
    const stats = [
      { rows: 100, sourceRows: [100, 0, 0] },
      { rows: 1, sourceRows: [0, 1, 0] }
    ]
    assert.deepEqual(detectHotBuckets(stats, 1.1), [])
  })
})
