'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { HyperLogLog, KllSketch, FrequencySketch, PriorityReservoir, BloomFilter, sketchFromJSON } = require('../shared/sketches')

describe('mergeable approximate analytics sketches', () => {
  it('merges Bloom filters without false negatives', () => {
    const left = new BloomFilter(32768, 5)
    const right = new BloomFilter(32768, 5)
    for (let value = 0; value < 500; value++) (value % 2 ? left : right).add(`key-${value}`)
    left.merge(right)
    for (let value = 0; value < 500; value++) assert.equal(left.has(`key-${value}`), true)
    const falsePositives = Array.from({ length: 500 }, (_, index) => left.has(`missing-${index}`)).filter(Boolean).length
    assert.ok(falsePositives / 500 < 0.05)
  })
  it('merges HLL partitions within its configured error envelope', () => {
    const sketches = Array.from({ length: 5 }, () => new HyperLogLog(12))
    for (let value = 0; value < 50000; value++) sketches[value % sketches.length].add(`key-${value}`)
    const merged = sketches.slice(1).reduce((left, right) => left.merge(right), sketches[0])
    const relativeError = Math.abs(merged.estimate() - 50000) / 50000
    assert.ok(relativeError < 0.05, `relative error ${relativeError}`)
    assert.equal(sketchFromJSON(merged.toJSON()).estimate(), merged.estimate())
  })

  it('merges KLL partitions with bounded median and p95 rank error', () => {
    const sketches = Array.from({ length: 4 }, () => new KllSketch(200))
    for (let value = 0; value < 10000; value++) sketches[value % 4].add(value)
    const merged = sketches.slice(1).reduce((left, right) => left.merge(right), sketches[0])
    assert.ok(Math.abs(merged.quantile(0.5) - 4999.5) < 250)
    assert.ok(Math.abs(merged.quantile(0.95) - 9499) < 300)
  })

  it('finds mergeable heavy hitters', () => {
    const left = new FrequencySketch(512, 4, 3)
    const right = new FrequencySketch(512, 4, 3)
    for (let index = 0; index < 500; index++) left.add('hot')
    for (let index = 0; index < 300; index++) right.add('hot')
    for (let index = 0; index < 200; index++) left.add('warm')
    left.merge(right)
    assert.deepEqual(left.topK(2).map(item => item.value), ['hot', 'warm'])
  })

  it('produces order-independent deterministic reservoir merges', () => {
    const left = new PriorityReservoir(20)
    const right = new PriorityReservoir(20)
    for (let value = 0; value < 1000; value++) (value % 2 ? left : right).add(value, `row-${value}`)
    const forward = new PriorityReservoir(20, left.entries).merge(right).values()
    const reverse = new PriorityReservoir(20, right.entries).merge(left).values()
    assert.deepEqual(forward, reverse)
  })
})
