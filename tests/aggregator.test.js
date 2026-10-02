'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { localGroupBy, uncombinedGroups } = require('../worker/src/services/aggregator')
const { mergeResults } = require('../coordinator/src/services/resultMerger')

describe('worker aggregator — alias-keyed partial state', () => {
  it('does not double-count SUM and AVG over the same column', () => {
    const groups = localGroupBy(
      [{ sales: '10' }, { sales: '20' }, { sales: '' }],
      [],
      [
        { function: 'SUM', column: 'sales', alias: 'sum_sales' },
        { function: 'AVG', column: 'sales', alias: 'avg_sales' },
        { function: 'COUNT', column: 'sales', alias: 'count_sales' },
        { function: 'COUNT', column: '*', alias: 'count_all' }
      ]
    )
    assert.equal(groups.length, 1)
    assert.deepEqual(groups[0].values, { sum_sales: 30, avg_sales: 30 })
    assert.deepEqual(groups[0].counts, { sum_sales: 2, avg_sales: 2, count_sales: 2, count_all: 3 })
    assert.deepEqual(groups[0].sums, { sales: 30 })
  })

  it('creates the SQL global aggregate group for empty input', () => {
    const [group] = localGroupBy([], [], [{ function: 'COUNT', column: '*', alias: 'n' }])
    assert.equal(group.group_key, '[]')
    assert.deepEqual(group.counts, { n: 0 })
  })

  it('does not collide composite group keys containing separators', () => {
    const groups = localGroupBy(
      [{ a: 'x|y', b: 'z' }, { a: 'x', b: 'y|z' }],
      ['a', 'b'],
      [{ function: 'COUNT', column: '*', alias: 'n' }]
    )
    assert.equal(groups.length, 2)
  })

  it('emits real row-level states when the combiner is disabled', () => {
    const aggregations = [
      { function: 'SUM', column: 'sales', alias: 'total' },
      { function: 'AVG', column: 'sales', alias: 'average' },
      { function: 'COUNT', column: '*', alias: 'rows' }
    ]
    const rows = [{ region: 'east', sales: '10' }, { region: 'east', sales: '20' }]
    const groups = uncombinedGroups(rows, ['region'], aggregations)
    assert.equal(groups.length, 2)
    assert.deepEqual(mergeResults([{ is_aggregated: true, groups }], { aggregations, limit: 0 }), [
      { region: 'east', total: 30, average: 15, rows: 2 }
    ])
  })

  it('matches direct aggregation across 100 generated partition layouts', () => {
    const aggregations = [
      { function: 'COUNT', column: '*', alias: 'rows' },
      { function: 'COUNT', column: 'value', alias: 'counted' },
      { function: 'SUM', column: 'value', alias: 'total' },
      { function: 'AVG', column: 'value', alias: 'average' },
      { function: 'MIN', column: 'value', alias: 'minimum' },
      { function: 'MAX', column: 'value', alias: 'maximum' }
    ]
    const plan = { aggregations, orderByColumn: 'group', orderByDirection: 'ASC', orderByType: 'string', limit: 0 }

    for (let size = 1; size <= 100; size++) {
      const rows = Array.from({ length: size }, (_, index) => ({
        group: ['a', 'b', 'c'][index % 3],
        value: index % 11 === 0 ? '' : String((index * 37) % 101)
      }))
      const partitions = Array.from({ length: 1 + (size % 7) }, () => [])
      rows.forEach((row, index) => partitions[index % partitions.length].push(row))
      const partials = partitions.map(partition => ({
        is_aggregated: true,
        groups: localGroupBy(partition, ['group'], aggregations)
      }))
      const actual = mergeResults(partials, plan)
      const expected = ['a', 'b', 'c'].map(group => {
        const groupRows = rows.filter(row => row.group === group)
        if (groupRows.length === 0) return null
        const values = groupRows.filter(row => row.value !== '').map(row => Number(row.value))
        const total = values.reduce((sum, value) => sum + value, 0)
        return {
          group,
          rows: groupRows.length,
          counted: groupRows.filter(row => row.value !== '').length,
          total: values.length > 0 ? total : null,
          average: values.length > 0 ? total / values.length : null,
          minimum: values.length > 0 ? Math.min(...values) : null,
          maximum: values.length > 0 ? Math.max(...values) : null
        }
      }).filter(Boolean)
      assert.deepEqual(actual, expected)
    }
  })
})
