'use strict'

const fs = require('fs/promises')
const { DuckDBInstance } = require('@duckdb/node-api')
const { tableFromArrays, tableToIPC } = require('apache-arrow')
const { acquireCachedPartition } = require('./cacheManager')
const { toUncombinedAggregationGroup } = require('./columnarExecutor')

function quoteIdentifier (value) { return `"${String(value).replace(/"/g, '""')}"` }
function quoteLiteral (value) { return `'${String(value).replace(/'/g, "''")}'` }

function aggregationGroup (row, request) {
  const groupValues = {}
  for (const column of request.group_by_columns || []) groupValues[column] = String(row[column] ?? '')
  const values = {}
  const counts = {}
  request.aggregations.forEach((aggregation, index) => {
    counts[aggregation.alias] = Number(row[`__c${index}`] || 0)
    if (aggregation.function !== 'COUNT' && row[`__v${index}`] !== null) values[aggregation.alias] = Number(row[`__v${index}`])
  })
  return {
    group_key: JSON.stringify((request.group_by_columns || []).map(column => groupValues[column])),
    count: Number(row.__rows || 0), sums: {}, values, counts, group_values: groupValues
  }
}

async function executeJoinTask (request, onResult) {
  const directory = `/tmp/queryforge-join-${request.task_id}`
  await fs.mkdir(directory, { recursive: true })
  const primaryPaths = request.primary_partition_paths?.length ? request.primary_partition_paths : [request.partition_path]
  const secondaryPaths = request.secondary_partition_paths || []
  const leases = []
  const download = async (objectPath, side, index) => {
    const lease = await acquireCachedPartition(
      objectPath, `${request.task_id}-${side}-${index}`, 'parquet',
      request.cache_level, request.cache_budget_bytes
    )
    leases.push(lease)
    return lease.path
  }
  const primary = []
  const secondary = []
  try {
    for (let index = 0; index < primaryPaths.length; index++) primary.push(await download(primaryPaths[index], 'primary', index))
    for (let index = 0; index < secondaryPaths.length; index++) secondary.push(await download(secondaryPaths[index], 'secondary', index))
    const instance = await DuckDBInstance.create(':memory:', {
      memory_limit: process.env.WORKER_MEMORY_LIMIT || '256MB', temp_directory: `${directory}/spill`,
      threads: process.env.WORKER_THREADS || '2'
    })
    const connection = await instance.connect()
    try {
      const list = paths => `[${paths.map(quoteLiteral).join(',')}]`
      await connection.run(`CREATE VIEW ${quoteIdentifier(request.primary_table_alias)} AS SELECT * FROM read_parquet(${list(primary)})`)
      await connection.run(`CREATE VIEW ${quoteIdentifier(request.secondary_table_alias)} AS SELECT * FROM read_parquet(${list(secondary)})`)
      const aggregated = (request.aggregations?.length || 0) > 0 || (request.group_by_columns?.length || 0) > 0
      const result = await connection.stream(request.execution_sql)
      const columnNames = result.columnNames()
      let pending = null
      for await (const rows of result.yieldRowObjectJson()) {
        if (pending) emit(pending, false)
        pending = rows
      }
      emit(pending || [], true)
      function emit (rows, complete) {
        const arrowColumns = {}
        if (!aggregated) for (const name of columnNames) arrowColumns[name] = rows.map(row => String(row[name] ?? ''))
        onResult({
          task_id: request.task_id,
          is_aggregated: aggregated,
          column_names: aggregated ? [] : columnNames,
          rows: [],
          groups: aggregated ? rows.map(row => request.disable_combiner
            ? toUncombinedAggregationGroup(row, request)
            : aggregationGroup(row, request)) : [],
          arrow_ipc: !aggregated && rows.length ? Buffer.from(tableToIPC(tableFromArrays(arrowColumns), 'stream')) : Buffer.alloc(0),
          is_complete: complete,
          rows_scanned: Number(request.partition_row_count || 0),
          bytes_scanned: Number(request.partition_byte_size || 0), bytes_skipped: 0,
          cache_hit: leases.length > 0 && leases.every(lease => lease.cacheHit),
          cache_level: leases.some(lease => lease.cacheLevel === 'MEMORY') ? 'MEMORY' : (leases[0]?.cacheLevel || 'NONE')
        })
      }
    } finally {
      connection.closeSync()
    }
  } finally {
    await Promise.all(leases.map(lease => lease.release()))
    await fs.rm(directory, { recursive: true, force: true })
  }
}

module.exports = { executeJoinTask }
