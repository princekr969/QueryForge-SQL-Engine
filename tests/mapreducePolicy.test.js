'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { percentile, speculationThresholdMs } = require('../coordinator/src/services/speculationPolicy')
const { normalizedLevel } = require('../worker/src/services/cacheManager')

describe('MapReduce runtime policy', () => {
  it('uses sibling p75 duration without dropping below the safety floor', () => {
    assert.equal(percentile([100, 200, 300, 400], 0.75), 400)
    assert.equal(speculationThresholdMs([100, 110, 120], 250), 250)
    assert.equal(speculationThresholdMs([200, 400, 600, 800], 250), 1200)
  })

  it('fails closed to no cache for unknown persistence levels', () => {
    assert.equal(normalizedLevel('memory_and_disk'), 'MEMORY_AND_DISK')
    assert.equal(normalizedLevel('remote_magic'), 'NONE')
  })
})
