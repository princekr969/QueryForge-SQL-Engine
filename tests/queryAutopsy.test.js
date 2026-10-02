'use strict'

const assert = require('node:assert/strict')
const { it } = require('node:test')
const { buildAutopsy } = require('../coordinator/src/services/queryAutopsy')

it('attributes a straggler and aggregates operator evidence', () => {
  const job = { id: 'job', status: 'completed', execution_time_ms: 1000, result_checksum: 'abc', plan_json: {} }
  const tasks = [
    { id: 'a', worker_id: 'w1', status: 'completed', started_at: '2026-01-01T00:00:00Z', completed_at: '2026-01-01T00:00:00.100Z', rows_scanned: 10, bytes_scanned: 100, transferred_bytes: 10 },
    { id: 'b', worker_id: 'w2', status: 'completed', started_at: '2026-01-01T00:00:00Z', completed_at: '2026-01-01T00:00:01Z', rows_scanned: 20, bytes_scanned: 200, transferred_bytes: 20 },
    { id: 'c', worker_id: 'w3', status: 'completed', started_at: '2026-01-01T00:00:00Z', completed_at: '2026-01-01T00:00:00.100Z', rows_scanned: 10, bytes_scanned: 100, transferred_bytes: 10 }
  ]
  const result = buildAutopsy(job, tasks)
  assert.equal(result.criticalPath.workerId, 'w2')
  assert.equal(result.totals.rowsScanned, 40)
  assert.equal(result.timeline[0].rowsPerSecond, 100)
  assert.equal(result.topOperators.length, 3)
  assert.equal(result.topOperators[0].workerId, 'w2')
  assert.equal(result.topOperators[0].durationMs, 1000)
  assert.equal(result.topOperators[0].scannedRows, 20)
  assert.equal(result.topOperators[0].wireBytes, 20)
  assert.equal(result.operators[0].scannedRows, 40)
  assert.equal(result.operators[0].bytesPerSecond, 400 / 1200 * 1000)
  assert.equal(result.costDomains.length, 7)
  assert.match(result.costDomainNote, /not ranked/)
  assert.ok(result.topCosts.every(item => item.ranked === false))
  assert.match(result.suggestions[0], /straggler/)
})
