'use strict'

const { DuckDBInstance } = require('@duckdb/node-api')
const fs = require('fs/promises')
const { tableFromArrays, tableToIPC } = require('apache-arrow')

function quoteIdentifier (value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

function quoteLiteral (value) {
  return `'${String(value).replace(/'/g, "''")}'`
}

function buildWhere (predicates) {
  if (!predicates || predicates.length === 0) return ''
  return ` WHERE ${predicates.map(predicate => {
    const value = predicate.type === 'number' ? String(Number(predicate.value)) : quoteLiteral(predicate.value)
    return `${quoteIdentifier(predicate.column)} ${predicate.operator} ${value}`
  }).join(' AND ')}`
}

function buildPlainSql (localPath, request) {
  const projection = !request.select_columns?.length || request.select_columns[0] === '*'
    ? '*'
    : request.select_columns.map(quoteIdentifier).join(', ')
  return `SELECT ${projection} FROM read_parquet(${quoteLiteral(localPath)})${buildWhere(request.predicates)}`
}

function buildAggregateSql (localPath, request) {
  const groups = request.group_by_columns || []
  const projections = groups.map(quoteIdentifier)
  projections.push('COUNT(*) AS "__rows"')
  request.aggregations.forEach((aggregation, index) => {
    const column = aggregation.column === '*' ? '*' : quoteIdentifier(aggregation.column)
    if (aggregation.function === 'COUNT') {
      projections.push(`COUNT(${column}) AS ${quoteIdentifier(`__c${index}`)}`)
    } else if (aggregation.function === 'AVG') {
      projections.push(`SUM(${column}) AS ${quoteIdentifier(`__v${index}`)}`)
      projections.push(`COUNT(${column}) AS ${quoteIdentifier(`__c${index}`)}`)
    } else {
      projections.push(`${aggregation.function}(${column}) AS ${quoteIdentifier(`__v${index}`)}`)
      projections.push(`COUNT(${column}) AS ${quoteIdentifier(`__c${index}`)}`)
    }
  })
  const groupBy = groups.length > 0 ? ` GROUP BY ${groups.map(quoteIdentifier).join(', ')}` : ''
  return `SELECT ${projections.join(', ')} FROM read_parquet(${quoteLiteral(localPath)})${buildWhere(request.predicates)}${groupBy}`
}

function buildUncombinedSql (localPath, request) {
  const projections = (request.group_by_columns || []).map(quoteIdentifier)
  request.aggregations.forEach((aggregation, index) => {
    if (aggregation.function === 'COUNT' && aggregation.column === '*') {
      projections.push(`1 AS ${quoteIdentifier(`__r${index}`)}`)
    } else {
      projections.push(`${quoteIdentifier(aggregation.column)} AS ${quoteIdentifier(`__r${index}`)}`)
    }
  })
  return `SELECT ${projections.join(', ')} FROM read_parquet(${quoteLiteral(localPath)})${buildWhere(request.predicates)}`
}

function estimatePruning (statsJson, predicates, fallbackBytes) {
  let stats
  try { stats = statsJson ? JSON.parse(statsJson) : null } catch { stats = null }
  if (!stats?.rowGroups?.length) return { bytesScanned: Number(fallbackBytes || 0), bytesSkipped: 0 }

  let total = 0
  let skipped = 0
  for (const rowGroup of stats.rowGroups) {
    const bytes = Number(rowGroup.compressedBytes || 0)
    total += bytes
    const impossible = (predicates || []).some(predicate => {
      const column = rowGroup.columns?.[predicate.column]
      if (!column || column.min === null || column.max === null) return false
      const convert = predicate.type === 'number' ? Number : String
      const min = convert(column.min)
      const max = convert(column.max)
      const value = convert(predicate.value)
      if (predicate.operator === '>') return max <= value
      if (predicate.operator === '>=') return max < value
      if (predicate.operator === '<') return min >= value
      if (predicate.operator === '<=') return min > value
      if (predicate.operator === '=') return value < min || value > max
      if (predicate.operator === '!=' || predicate.operator === '<>') return min === value && max === value
      return false
    })
    if (impossible) skipped += bytes
  }
  return { bytesScanned: total - skipped, bytesSkipped: skipped }
}

function toAggregationGroup (row, request) {
  const groupValues = {}
  for (const column of request.group_by_columns || []) groupValues[column] = String(row[column] ?? '')
  const values = {}
  const counts = {}
  request.aggregations.forEach((aggregation, index) => {
    counts[aggregation.alias] = Number(row[`__c${index}`] || 0)
    if (aggregation.function !== 'COUNT' && row[`__v${index}`] !== null) {
      values[aggregation.alias] = Number(row[`__v${index}`])
    }
  })
  return {
    group_key: JSON.stringify((request.group_by_columns || []).map(column => groupValues[column])),
    count: Number(row.__rows || 0),
    sums: {},
    values,
    counts,
    group_values: groupValues
  }
}

function toUncombinedAggregationGroup (row, request) {
  const groupValues = {}
  for (const column of request.group_by_columns || []) groupValues[column] = String(row[column] ?? '')
  const values = {}
  const counts = {}
  request.aggregations.forEach((aggregation, index) => {
    const raw = row[`__r${index}`]
    const present = aggregation.column === '*' || (raw !== null && raw !== undefined)
    counts[aggregation.alias] = present ? 1 : 0
    if (aggregation.function !== 'COUNT' && present) values[aggregation.alias] = Number(raw)
  })
  return {
    group_key: JSON.stringify((request.group_by_columns || []).map(column => groupValues[column])),
    count: 1,
    sums: {},
    values,
    counts,
    group_values: groupValues
  }
}

async function executeColumnarTask (localPath, request, onResult) {
  const spillDirectory = `/tmp/queryforge-spill-${request.task_id}`
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: process.env.WORKER_MEMORY_LIMIT || '256MB',
    temp_directory: spillDirectory,
    threads: process.env.WORKER_THREADS || '2'
  })
  const connection = await instance.connect()
  const aggregated = (request.group_by_columns?.length || 0) > 0 || (request.aggregations?.length || 0) > 0
  const sql = aggregated
    ? (request.disable_combiner ? buildUncombinedSql(localPath, request) : buildAggregateSql(localPath, request))
    : buildPlainSql(localPath, request)
  const rowsScanned = Number(request.partition_row_count || 0)
  const { bytesScanned, bytesSkipped } = estimatePruning(
    request.partition_stats_json,
    request.predicates,
    request.partition_byte_size
  )

  try {
    const result = await connection.stream(sql)
    const columnNames = result.columnNames()
    let pending = null

    for await (const rows of result.yieldRowObjectJson()) {
      if (pending) emit(pending, false)
      pending = rows
    }
    emit(pending || [], true)

    function emit (rows, isComplete) {
      if (aggregated) {
        const groups = rows.map(row => request.disable_combiner
          ? toUncombinedAggregationGroup(row, request)
          : toAggregationGroup(row, request))
        if (request.disable_combiner && groups.length === 0 &&
            (request.group_by_columns || []).length === 0 && isComplete) {
          groups.push({
            group_key: '[]', count: 0, sums: {}, values: {},
            counts: Object.fromEntries((request.aggregations || []).map(item => [item.alias, 0])),
            group_values: {}
          })
        }
        onResult({
          task_id: request.task_id,
          is_aggregated: true,
          column_names: [],
          rows: [],
          groups,
          is_complete: isComplete,
          rows_scanned: rowsScanned,
          bytes_scanned: bytesScanned,
          bytes_skipped: bytesSkipped
        })
      } else {
        const arrowColumns = {}
        for (const column of columnNames) {
          arrowColumns[column] = rows.map(row => String(row[column] ?? ''))
        }
        const arrowIpc = rows.length > 0
          ? Buffer.from(tableToIPC(tableFromArrays(arrowColumns), 'stream'))
          : Buffer.alloc(0)
        onResult({
          task_id: request.task_id,
          is_aggregated: false,
          column_names: columnNames,
          rows: [],
          arrow_ipc: arrowIpc,
          groups: [],
          is_complete: isComplete,
          rows_scanned: rowsScanned,
          bytes_scanned: bytesScanned,
          bytes_skipped: bytesSkipped
        })
      }
    }
  } finally {
    connection.closeSync()
    await fs.rm(spillDirectory, { recursive: true, force: true })
  }
}

module.exports = {
  executeColumnarTask,
  buildPlainSql,
  buildAggregateSql,
  buildUncombinedSql,
  toUncombinedAggregationGroup,
  estimatePruning
}
