'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { partialResultsChecksum } = require('../coordinator/src/services/resultChecksum')

const plan = {
  aggregations: [{ alias: 'rows', column: '*', function: 'COUNT' }],
  groupByColumns: ['region'],
  selectColumns: ['region'],
  orderByColumn: null,
  limit: 0
}

function partial (groups) {
  return [{ is_aggregated: true, groups }]
}

test('partial lineage checksums ignore unordered aggregate serialization', () => {
  const east = { group_key: 'east', count: 2, counts: { rows: 2 }, values: {}, group_values: { region: 'east' } }
  const west = { values: {}, group_values: { region: 'west' }, counts: { rows: 3 }, count: 3, group_key: 'west' }
  const reorderedWest = { group_key: 'west', count: 3, counts: { rows: 3 }, values: {}, group_values: { region: 'west' } }

  assert.equal(
    partialResultsChecksum(partial([east, west]), plan),
    partialResultsChecksum(partial([reorderedWest, east]), plan)
  )
  assert.notEqual(
    partialResultsChecksum(partial([east, west]), plan),
    partialResultsChecksum(partial([{ ...reorderedWest, count: 4, counts: { rows: 4 } }, east]), plan)
  )
})
