'use strict'

/**
 * aggregator.js
 * Local GROUP BY partial aggregation on a worker.
 * Builds a hash map: groupKey → { count, sums, groupValues }
 * The coordinator merges these from all workers in the final reduce step.
 */

/**
 * Create an empty aggregation state.
 */
function createAggState () {
  return {
    count:       0,
    sums:        {},
    values:      {},
    counts:      {},
    groupValues: {}
  }
}

function initializeAggregations (state, aggregations) {
  for (const aggregation of aggregations) state.counts[aggregation.alias] = 0
  return state
}

/**
 * Update an aggregation state with a new matching row.
 *
 * @param {object}   state        - current aggregation state for this group key
 * @param {object}   row          - CSV row (all values are strings)
 * @param {object[]} aggregations - [{ function, column, alias }]
 */
function updateAggState (state, row, aggregations) {
  state.count++

  // Legacy protobuf state is retained during the rolling migration. A column
  // is accumulated once per row even when both SUM and AVG reference it.
  const legacySummedColumns = new Set()

  for (const agg of aggregations) {
    const alias = agg.alias
    if (agg.function === 'COUNT') {
      if (agg.column === '*' || (row[agg.column] !== undefined && row[agg.column] !== null && row[agg.column] !== '')) {
        state.counts[alias]++
      }
      continue
    }

    const col = agg.column
    if (!col || col === '*') continue
    const numVal = parseFloat(row[col])
    if (isNaN(numVal)) continue

    if (agg.function === 'SUM') {
      state.values[alias] = (state.values[alias] || 0) + numVal
      state.counts[alias]++
    } else if (agg.function === 'AVG') {
      state.values[alias] = (state.values[alias] || 0) + numVal
      state.counts[alias]++
    } else if (agg.function === 'MAX') {
      if (state.values[alias] === undefined || numVal > state.values[alias]) {
        state.values[alias] = numVal
      }
      state.counts[alias]++
    } else if (agg.function === 'MIN') {
      if (state.values[alias] === undefined || numVal < state.values[alias]) {
        state.values[alias] = numVal
      }
      state.counts[alias]++
    }

    if ((agg.function === 'SUM' || agg.function === 'AVG') && !legacySummedColumns.has(col)) {
      state.sums[col] = (state.sums[col] || 0) + numVal
      legacySummedColumns.add(col)
    } else if (agg.function === 'MAX') {
      const key = `__max__${col}`
      if (state.sums[key] === undefined || numVal > state.sums[key]) state.sums[key] = numVal
    } else if (agg.function === 'MIN') {
      const key = `__min__${col}`
      if (state.sums[key] === undefined || numVal < state.sums[key]) state.sums[key] = numVal
    }
  }
}

/**
 * Run local GROUP BY aggregation over an array of filtered rows.
 *
 * @param {object[]} rows           - rows that passed predicate filters
 * @param {string[]} groupByColumns - GROUP BY column names
 * @param {object[]} aggregations   - aggregation function descriptors
 * @returns {object[]} AggregationGroup-shaped objects ready for proto serialisation
 */
function localGroupBy (rows, groupByColumns, aggregations) {
  const hashMap = new Map()  // groupKey → aggregation state

  // A global aggregate has one group even when its input is empty. This is
  // required for COUNT(*) to return 0 rather than no rows.
  if (groupByColumns.length === 0) hashMap.set('[]', initializeAggregations(createAggState(), aggregations))

  for (const row of rows) {
    // JSON encoding prevents collisions such as ["a|b", "c"] vs ["a", "b|c"].
    const groupKey = JSON.stringify(groupByColumns.map(col => String(row[col] ?? '')))

    if (!hashMap.has(groupKey)) {
      const state = initializeAggregations(createAggState(), aggregations)

      // Record the group-by column values for output reconstruction
      for (const col of groupByColumns) {
        state.groupValues[col] = String(row[col] ?? '')
      }

      hashMap.set(groupKey, state)
    }

    updateAggState(hashMap.get(groupKey), row, aggregations)
  }

  return Array.from(hashMap.entries()).map(([groupKey, state]) => ({
    group_key:    groupKey,
    count:        state.count,
    sums:         state.sums,
    values:       state.values,
    counts:       state.counts,
    group_values: state.groupValues
  }))
}

/**
 * Deliberately bypass the map-side combiner while retaining the exact same
 * mergeable wire contract. This is used by the MapReduce teaching/ablation
 * mode, so the extra network traffic is real rather than an estimated metric.
 */
function uncombinedGroups (rows, groupByColumns, aggregations) {
  if (rows.length === 0) return groupByColumns.length === 0 ? localGroupBy([], groupByColumns, aggregations) : []
  return rows.flatMap(row => localGroupBy([row], groupByColumns, aggregations))
}

module.exports = { localGroupBy, uncombinedGroups }
