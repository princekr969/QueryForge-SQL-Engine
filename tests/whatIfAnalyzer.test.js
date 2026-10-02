'use strict'

const assert = require('node:assert/strict')
const { it } = require('node:test')
const { predictWhatIf } = require('../coordinator/src/services/whatIfAnalyzer')

it('labels what-if output as a projection with evidence and assumptions', () => {
  const result = predictWhatIf(
    { execution_time_ms: 1000, resource_budget_json: {}, plan_json: {} },
    [1, 2, 3, 4].map(index => ({ worker_id: `w${index}`, is_winner: true, transferred_bytes: 10, bytes_scanned: 100 })),
    { workers: 8, combiner: true, cacheWarm: true }
  )
  assert.ok(result.predictedExecutionTimeMs < 1000)
  assert.equal(result.confidence, 'medium')
  assert.match(result.model, /projection/)
  assert.ok(result.assumptions.length >= 3)
})

it('rejects unvalidated alternate-plan controls', () => {
  const job = { execution_time_ms: 100, resource_budget_json: {}, plan_json: {} }
  assert.throws(() => predictWhatIf(job, [], { joinStrategy: 'magic' }), /joinStrategy/)
  assert.throws(() => predictWhatIf(job, [], { cacheWarm: 'yes' }), /cacheWarm/)
})
