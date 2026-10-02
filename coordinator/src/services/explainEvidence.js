'use strict'

function canonicalSql (sql) {
  // Be deliberately conservative: collapsing whitespace can change string
  // literals, so only ignore surrounding space and one statement terminator.
  return String(sql || '').trim().replace(/;\s*$/, '').trim()
}

function canonicalBindings (bindings = {}) {
  return JSON.stringify(Object.entries(bindings || {})
    .map(([name, datasetId]) => [String(name), String(datasetId)])
    .sort(([left], [right]) => left.localeCompare(right)))
}

function matchesMeasuredExecution (measured, sql, datasetIds = {}) {
  return canonicalSql(measured.sql_query) === canonicalSql(sql) &&
    canonicalBindings(measured.query_context_json?.datasetIds) === canonicalBindings(datasetIds)
}

module.exports = { canonicalSql, canonicalBindings, matchesMeasuredExecution }
