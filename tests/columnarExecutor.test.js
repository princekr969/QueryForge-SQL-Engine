'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const {
  buildPlainSql, buildAggregateSql, buildUncombinedSql,
  toUncombinedAggregationGroup, estimatePruning
} = require('../worker/src/services/columnarExecutor')

describe('columnar execution planning', () => {
  it('builds projected, pushed-down Parquet scans', () => {
    const sql = buildPlainSql('/tmp/data.parquet', {
      select_columns: ['region', 'sales'],
      predicates: [{ column: 'sales', operator: '>=', value: '50', type: 'number' }]
    })
    assert.match(sql, /SELECT "region", "sales" FROM read_parquet/)
    assert.match(sql, /WHERE "sales" >= 50/)
  })

  it('builds mergeable local AVG state rather than averaging averages', () => {
    const sql = buildAggregateSql('/tmp/data.parquet', {
      group_by_columns: ['region'],
      predicates: [],
      aggregations: [{ function: 'AVG', column: 'sales', alias: 'average' }]
    })
    assert.match(sql, /SUM\("sales"\) AS "__v0"/)
    assert.match(sql, /COUNT\("sales"\) AS "__c0"/)
    assert.match(sql, /GROUP BY "region"/)
  })

  it('accounts for row groups eliminated by min/max predicates', () => {
    const stats = JSON.stringify({ rowGroups: [
      { compressedBytes: 100, columns: { sales: { min: '1', max: '10' } } },
      { compressedBytes: 120, columns: { sales: { min: '50', max: '100' } } }
    ] })
    assert.deepEqual(
      estimatePruning(stats, [{ column: 'sales', operator: '>', value: '40', type: 'number' }], 300),
      { bytesScanned: 120, bytesSkipped: 100 }
    )
  })

  it('builds and decodes an uncombined row-level aggregate state', () => {
    const request = {
      group_by_columns: ['region'], predicates: [],
      aggregations: [
        { function: 'AVG', column: 'sales', alias: 'average' },
        { function: 'COUNT', column: '*', alias: 'rows' }
      ]
    }
    const sql = buildUncombinedSql('/tmp/data.parquet', request)
    assert.match(sql, /"sales" AS "__r0"/)
    assert.match(sql, /1 AS "__r1"/)
    assert.doesNotMatch(sql, /GROUP BY/)
    assert.deepEqual(toUncombinedAggregationGroup({ region: 'east', __r0: 12, __r1: 1 }, request), {
      group_key: '["east"]', count: 1, sums: {}, values: { average: 12 },
      counts: { average: 1, rows: 1 }, group_values: { region: 'east' }
    })
  })
})
