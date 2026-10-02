'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const {
  parseStandingStatement, normalizeStreamConfig, fixedWindows,
  createState, mergeEventState, stateResult, sessionMatchBounds, isTooLate
} = require('../coordinator/src/services/streamingEngine')

describe('streaming SQL planning', () => {
  it('parses a course-style tumbling standing query with sub-1% HLL precision', () => {
    const config = parseStandingStatement("REGISTER QUERY live AS SELECT COUNT(DISTINCT user_id), SUM(revenue) FROM clicks WINDOW TUMBLE(ts, INTERVAL '1' MINUTE) WATERMARK INTERVAL '5' SECOND")
    assert.equal(config.name, 'live')
    assert.equal(config.topic, 'clicks')
    assert.equal(config.windowType, 'TUMBLE')
    assert.equal(config.sizeMs, 60000)
    assert.equal(config.allowedLatenessMs, 5000)
    assert.equal(config.hllPrecision, 14)
  })

  it('assigns every event to one tumble and all overlapping hop windows', () => {
    const tumble = normalizeStreamConfig({ name: 't', topic: 'x', windowType: 'TUMBLE', sizeMs: 1000 })
    assert.deepEqual(fixedWindows(2500, tumble), [{ start: 2000, end: 3000 }])
    const hop = normalizeStreamConfig({ name: 'h', topic: 'x', windowType: 'HOP', slideMs: 1000, sizeMs: 3000 })
    assert.deepEqual(fixedWindows(2500, hop), [
      { start: 2000, end: 5000 }, { start: 1000, end: 4000 }, { start: 0, end: 3000 }
    ])
  })

  it('does not apply the session inactivity gap twice', () => {
    const bounds = sessionMatchBounds(2000, 1000)
    assert.deepEqual(bounds, { latestStart: 3000, earliestEnd: 2000 })
    const existing = { start: 0, end: 1500 }
    assert.equal(existing.start <= bounds.latestStart && existing.end >= bounds.earliestEnd, false)
  })

  it('audits an event exactly at the finalized watermark boundary', () => {
    assert.equal(isTooLate(999, 1000), true)
    assert.equal(isTooLate(1000, 1000), true)
    assert.equal(isTooLate(1001, 1000), false)
    assert.equal(isTooLate(0, 0), false)
  })

  it('rejects HLL configurations whose advertised error exceeds one percent', () => {
    assert.throws(() => normalizeStreamConfig({ name: 'bad', topic: 'x', hllPrecision: 12 }), /14-16/)
  })

  it('reports configured and observed HLL/CMS error in bounded teaching mode', () => {
    const config = normalizeStreamConfig({
      name: 'error_lab', topic: 'error_lab', windowType: 'TUMBLE', sizeMs: 1000,
      distinctColumn: 'user', heavyHitterColumn: 'url', hllPrecision: 14,
      trackExactEvaluation: true
    })
    let state = createState(config)
    for (const event of [
      { user: 'a', url: '/home' }, { user: 'b', url: '/home' },
      { user: 'c', url: '/search' }, { user: 'a', url: '/home' }
    ]) state = mergeEventState(state, event, config)
    const result = stateResult(state, config)
    assert.equal(result.distinctExact, 3)
    assert.ok(result.distinctConfiguredError < 0.01)
    assert.ok(result.distinctObservedError >= 0)
    assert.ok(result.frequencyConfiguredError < 0.01)
    assert.equal(result.frequencyObservedError, 0)
    assert.deepEqual(result.exactTopItems[0], { value: '/home', count: 3 })
  })
})
