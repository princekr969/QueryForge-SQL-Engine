'use strict'

const { tableFromIPC } = require('apache-arrow')

/**
 * resultMerger.js
 * Merges partial results from all workers into a final result set.
 *
 * MAX/MIN travel inside the sums map with __max__<col> and __min__<col> keys.
 * This avoids needing new proto fields — sums map<string,double> carries them.
 */

function mergeResults (partialResults, plan) {
  if (partialResults.length === 0) return []

  const isAggregated = partialResults.some(pr => pr.is_aggregated)
  let finalRows = isAggregated
    ? mergeAggregated(partialResults, plan)
    : mergePlainRows(partialResults)

  // ORDER BY
  if (plan.orderByColumn) {
    const col = plan.orderByColumn
    const asc = plan.orderByDirection !== 'DESC'
    finalRows.sort((a, b) => {
      if (plan.orderByType === 'number') {
        const an = Number(a[col])
        const bn = Number(b[col])
        if (!Number.isNaN(an) && !Number.isNaN(bn)) return asc ? an - bn : bn - an
      }
      const as = String(a[col] || '')
      const bs = String(b[col] || '')
      if (asc) return as < bs ? -1 : as > bs ? 1 : 0
      return bs < as ? -1 : bs > as ? 1 : 0
    })
  }

  // LIMIT
  if (plan.limit > 0) {
    finalRows = finalRows.slice(0, plan.limit)
  }

  return finalRows
}

function mergePlainRows (partialResults) {
  const allRows = []
  for (const pr of partialResults) {
    if (pr.arrow_ipc && pr.arrow_ipc.length > 0) {
      const table = tableFromIPC(pr.arrow_ipc)
      for (const row of table.toArray()) allRows.push(row.toJSON())
      continue
    }
    if (!pr.rows || pr.rows.length === 0) continue
    const colNames = pr.column_names || []
    for (const row of pr.rows) {
      const values = row.values || []
      const obj = {}
      colNames.forEach((col, i) => { obj[col] = values[i] !== undefined ? values[i] : null })
      allRows.push(obj)
    }
  }
  return allRows
}

function mergeAggregated (partialResults, plan) {
  const finalMap = new Map()

  for (const pr of partialResults) {
    if (!pr.groups || pr.groups.length === 0) continue

    for (const group of pr.groups) {
      const key = group.group_key
      if (!finalMap.has(key)) {
        finalMap.set(key, { rowCount: 0, sums: {}, aggregates: {}, groupValues: {} })
      }
      const merged = finalMap.get(key)

      // Legacy row count is retained for partial results produced by workers
      // that predate alias-keyed aggregate state.
      merged.rowCount += Number(group.count) || 0

      // Preserve legacy sums for rolling compatibility.
      if (group.sums) {
        Object.entries(group.sums).forEach(([col, partialVal]) => {
          const n = Number(partialVal) || 0
          if (col.startsWith('__max__')) {
            if (merged.sums[col] === undefined || n > merged.sums[col]) {
              merged.sums[col] = n
            }
          } else if (col.startsWith('__min__')) {
            if (merged.sums[col] === undefined || n < merged.sums[col]) {
              merged.sums[col] = n
            }
          } else {
            merged.sums[col] = (merged.sums[col] || 0) + n
          }
        })
      }

      for (const agg of plan.aggregations) {
        if (!merged.aggregates[agg.alias]) {
          merged.aggregates[agg.alias] = { value: undefined, count: 0, hasV2: false }
        }
        const target = merged.aggregates[agg.alias]
        const hasValue = group.values && Object.prototype.hasOwnProperty.call(group.values, agg.alias)
        const hasCount = group.counts && Object.prototype.hasOwnProperty.call(group.counts, agg.alias)

        if (hasValue || hasCount) {
          target.hasV2 = true
          const value = hasValue ? Number(group.values[agg.alias]) : undefined
          const count = hasCount ? Number(group.counts[agg.alias]) || 0 : 0

          if (agg.function === 'COUNT') {
            target.count += count
          } else if (agg.function === 'SUM' || agg.function === 'AVG') {
            if (hasValue) target.value = (target.value || 0) + value
            target.count += count
          } else if (agg.function === 'MAX' && hasValue) {
            if (target.value === undefined || value > target.value) target.value = value
            target.count += count
          } else if (agg.function === 'MIN' && hasValue) {
            if (target.value === undefined || value < target.value) target.value = value
            target.count += count
          }
        }
      }

      // Copy group_values
      if (group.group_values) {
        Object.entries(group.group_values).forEach(([col, val]) => {
          merged.groupValues[col] = val
        })
      }
    }
  }

  // Build output rows
  const outputRows = []
  for (const merged of finalMap.values()) {
    const row = {}

    // Group-by column values
    Object.entries(merged.groupValues).forEach(([col, val]) => { row[col] = val })

    // Aggregation results
    for (const agg of plan.aggregations) {
      const state = merged.aggregates[agg.alias]
      if (state?.hasV2) {
        if (agg.function === 'COUNT') row[agg.alias] = state.count
        else if (agg.function === 'SUM') row[agg.alias] = state.count > 0 ? state.value : null
        else if (agg.function === 'AVG') row[agg.alias] = state.count > 0 ? state.value / state.count : null
        else row[agg.alias] = state.count > 0 ? state.value : null
      } else if (agg.function === 'COUNT') {
        row[agg.alias] = merged.rowCount
      } else if (agg.function === 'SUM') {
        row[agg.alias] = merged.sums[agg.column] ?? null
      } else if (agg.function === 'AVG') {
        const total = merged.sums[agg.column]
        row[agg.alias] = total !== undefined && merged.rowCount > 0 ? total / merged.rowCount : null
      } else if (agg.function === 'MAX') {
        row[agg.alias] = merged.sums[`__max__${agg.column}`] ?? null
      } else if (agg.function === 'MIN') {
        row[agg.alias] = merged.sums[`__min__${agg.column}`] ?? null
      }
    }

    outputRows.push(row)
  }

  return outputRows
}

module.exports = { mergeResults }
