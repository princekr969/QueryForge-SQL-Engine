'use strict'

/**
 * Unit tests for resultMerger.js
 * Tests MapReduce-style partial aggregation merging logic.
 *
 * Run with: node --test tests/resultMerger.test.js
 */

const { mergeResults } = require('../coordinator/src/services/resultMerger')
const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { tableFromArrays, tableToIPC } = require('../coordinator/node_modules/apache-arrow')

describe('resultMerger — plain row merging', () => {
  it('merges rows from multiple workers into a single result set', () => {
    const partialResults = [
      {
        is_aggregated: false,
        column_names: ['name', 'salary'],
        rows: [
          { values: ['Alice', '60000'] },
          { values: ['Bob', '70000'] }
        ]
      },
      {
        is_aggregated: false,
        column_names: ['name', 'salary'],
        rows: [
          { values: ['Charlie', '80000'] }
        ]
      }
    ]
    const plan = { aggregations: [], orderByColumn: '', orderByDirection: 'ASC', limit: 0 }
    const result = mergeResults(partialResults, plan)
    assert.equal(result.length, 3)
    assert.equal(result[0].name, 'Alice')
    assert.equal(result[2].name, 'Charlie')
  })

  it('decodes Arrow IPC batches from columnar workers', () => {
    const arrowIpc = Buffer.from(tableToIPC(tableFromArrays({
      name: ['Alice', 'Bob'],
      salary: ['60000', '70000']
    }), 'stream'))
    const plan = { aggregations: [], orderByColumn: '', orderByDirection: 'ASC', limit: 0 }
    assert.deepEqual(mergeResults([{ is_aggregated: false, arrow_ipc: arrowIpc }], plan), [
      { name: 'Alice', salary: '60000' },
      { name: 'Bob', salary: '70000' }
    ])
  })

  it('applies LIMIT correctly', () => {
    const partialResults = [
      {
        is_aggregated: false,
        column_names: ['name'],
        rows: [{ values: ['A'] }, { values: ['B'] }, { values: ['C'] }]
      }
    ]
    const plan = { aggregations: [], orderByColumn: '', orderByDirection: 'ASC', limit: 2 }
    const result = mergeResults(partialResults, plan)
    assert.equal(result.length, 2)
  })

  it('uses catalog type for deterministic string ordering', () => {
    const partialResults = [{
      is_aggregated: false,
      column_names: ['code'],
      rows: [{ values: ['10'] }, { values: ['2'] }]
    }]
    const plan = { aggregations: [], orderByColumn: 'code', orderByDirection: 'ASC', orderByType: 'string', limit: 0 }
    assert.deepEqual(mergeResults(partialResults, plan).map(row => row.code), ['10', '2'])
  })

  it('returns empty array when no partial results', () => {
    const result = mergeResults([], { aggregations: [], orderByColumn: '', limit: 0 })
    assert.deepEqual(result, [])
  })
})

describe('resultMerger — aggregated merging (MapReduce)', () => {
  it('merges alias-keyed SUM and AVG independently with null-aware counts', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: '[]', count: 3, sums: { sales: 30 }, values: { sum_sales: 30, avg_sales: 30 }, counts: { sum_sales: 2, avg_sales: 2 }, group_values: {} }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: '[]', count: 2, sums: { sales: 70 }, values: { sum_sales: 70, avg_sales: 70 }, counts: { sum_sales: 2, avg_sales: 2 }, group_values: {} }]
      }
    ]
    const plan = {
      aggregations: [
        { function: 'SUM', column: 'sales', alias: 'sum_sales' },
        { function: 'AVG', column: 'sales', alias: 'avg_sales' }
      ],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    assert.deepEqual(mergeResults(partialResults, plan), [{ sum_sales: 100, avg_sales: 25 }])
  })

  it('returns COUNT 0 and null for other aggregates on empty global input', () => {
    const partialResults = [{
      is_aggregated: true,
      groups: [{ group_key: '[]', count: 0, sums: {}, values: {}, counts: {}, group_values: {} }]
    }]
    const plan = {
      aggregations: [
        { function: 'COUNT', column: '*', alias: 'n' },
        { function: 'SUM', column: 'sales', alias: 'sum_sales' },
        { function: 'AVG', column: 'sales', alias: 'avg_sales' }
      ],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    assert.deepEqual(mergeResults(partialResults, plan), [{ n: 0, sum_sales: null, avg_sales: null }])
  })

  it('correctly merges COUNT from multiple workers', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: 'Engineering', count: 100, sums: {}, group_values: { department: 'Engineering' } }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: 'Engineering', count: 150, sums: {}, group_values: { department: 'Engineering' } }]
      }
    ]
    const plan = {
      aggregations: [{ function: 'COUNT', column: '*', alias: 'total' }],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    assert.equal(result.length, 1)
    assert.equal(result[0].total, 250)
  })

  it('correctly merges SUM from multiple workers', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { salary: 130000 }, group_values: { department: 'Eng' } }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 1, sums: { salary: 80000 }, group_values: { department: 'Eng' } }]
      }
    ]
    const plan = {
      aggregations: [{ function: 'SUM', column: 'salary', alias: 'total_salary' }],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    assert.equal(result[0].total_salary, 210000)
  })

  it('correctly computes AVG as total_sum / total_count across workers', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { salary: 140000 }, group_values: { department: 'Eng' } }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { salary: 140000 }, group_values: { department: 'Eng' } }]
      }
    ]
    const plan = {
      aggregations: [{ function: 'AVG', column: 'salary', alias: 'avg_salary' }],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    // AVG = (140000+140000) / (2+2) = 70000 — NOT average of averages
    assert.equal(result[0].avg_salary, 70000)
  })

  it('correctly merges MAX — keeps highest value across workers', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { '__max__salary': 90000 }, group_values: { department: 'Eng' } }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { '__max__salary': 120000 }, group_values: { department: 'Eng' } }]
      }
    ]
    const plan = {
      aggregations: [{ function: 'MAX', column: 'salary', alias: 'max_salary' }],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    assert.equal(result[0].max_salary, 120000)
  })

  it('correctly merges MIN — keeps lowest value across workers', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { '__min__salary': 40000 }, group_values: { department: 'Eng' } }]
      },
      {
        is_aggregated: true,
        groups: [{ group_key: 'Eng', count: 2, sums: { '__min__salary': 30000 }, group_values: { department: 'Eng' } }]
      }
    ]
    const plan = {
      aggregations: [{ function: 'MIN', column: 'salary', alias: 'min_salary' }],
      orderByColumn: '', orderByDirection: 'ASC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    assert.equal(result[0].min_salary, 30000)
  })

  it('applies ORDER BY DESC on aggregated results', () => {
    const partialResults = [
      {
        is_aggregated: true,
        groups: [
          { group_key: 'Eng', count: 300, sums: {}, group_values: { department: 'Eng' } },
          { group_key: 'HR', count: 100, sums: {}, group_values: { department: 'HR' } }
        ]
      }
    ]
    const plan = {
      aggregations: [{ function: 'COUNT', column: '*', alias: 'total' }],
      orderByColumn: 'total', orderByDirection: 'DESC', limit: 0
    }
    const result = mergeResults(partialResults, plan)
    assert.equal(result[0].department, 'Eng')
    assert.equal(result[1].department, 'HR')
  })
})
