'use strict'

const assert = require('node:assert/strict')
const { test } = require('node:test')
const { canonicalSql, matchesMeasuredExecution } = require('../coordinator/src/services/explainEvidence')

test('measured EXPLAIN evidence is bound to SQL and every dataset snapshot', () => {
  const measured = {
    sql_query: ' SELECT region, COUNT(*) FROM sales GROUP BY region; ',
    query_context_json: { datasetIds: { customers: 'snapshot-b', products: 'snapshot-c' } }
  }
  assert.equal(canonicalSql(measured.sql_query), 'SELECT region, COUNT(*) FROM sales GROUP BY region')
  assert.equal(matchesMeasuredExecution(
    measured,
    'SELECT region, COUNT(*) FROM sales GROUP BY region',
    { products: 'snapshot-c', customers: 'snapshot-b' }
  ), true)
  assert.equal(matchesMeasuredExecution(
    measured,
    'SELECT region, SUM(amount) FROM sales GROUP BY region',
    { products: 'snapshot-c', customers: 'snapshot-b' }
  ), false)
  assert.equal(matchesMeasuredExecution(
    measured,
    'SELECT region, COUNT(*) FROM sales GROUP BY region',
    { products: 'different-snapshot', customers: 'snapshot-b' }
  ), false)
})
