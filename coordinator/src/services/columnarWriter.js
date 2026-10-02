'use strict'

const fs = require('fs/promises')
const os = require('os')
const path = require('path')
const { DuckDBInstance } = require('@duckdb/node-api')

function quoteIdentifier (value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

function quoteLiteral (value) {
  return `'${String(value).replace(/'/g, "''")}'`
}

async function convertCsvFileToParquet (csvPath, parquetPath, columnNames, taskKey) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), `queryforge-columnar-${taskKey}-`))
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: process.env.INGEST_MEMORY_LIMIT || '256MB',
    temp_directory: tempDir,
    threads: '2'
  })
  const connection = await instance.connect()

  try {
    await connection.run(
      `COPY (SELECT * FROM read_csv_auto(${quoteLiteral(csvPath)}, header = true, nullstr = ''))
       TO ${quoteLiteral(parquetPath)}
       (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)`
    )

    const projection = columnNames.flatMap((column, index) => {
      const identifier = quoteIdentifier(column)
      return [
        `COUNT(*) - COUNT(${identifier}) AS ${quoteIdentifier(`null_${index}`)}`,
        `MIN(${identifier}) AS ${quoteIdentifier(`min_${index}`)}`,
        `MAX(${identifier}) AS ${quoteIdentifier(`max_${index}`)}`,
        `APPROX_COUNT_DISTINCT(${identifier}) AS ${quoteIdentifier(`distinct_${index}`)}`
      ]
    }).join(', ')
    const statsReader = await connection.runAndReadAll(
      `SELECT COUNT(*) AS row_count, ${projection} FROM read_parquet(${quoteLiteral(parquetPath)})`
    )
    const fileStats = statsReader.getRowObjectsJson()[0]
    const metadataReader = await connection.runAndReadAll(
      `SELECT row_group_id, row_group_num_rows, row_group_compressed_bytes,
              path_in_schema, stats_min, stats_max, stats_null_count
       FROM parquet_metadata(${quoteLiteral(parquetPath)})
       ORDER BY row_group_id, column_id`
    )
    const rowGroupMap = new Map()
    for (const row of metadataReader.getRowObjectsJson()) {
      const index = Number(row.row_group_id)
      if (!rowGroupMap.has(index)) {
        rowGroupMap.set(index, {
          index,
          rowCount: Number(row.row_group_num_rows),
          compressedBytes: Number(row.row_group_compressed_bytes),
          columns: {}
        })
      }
      rowGroupMap.get(index).columns[row.path_in_schema] = {
        min: row.stats_min,
        max: row.stats_max,
        nullCount: Number(row.stats_null_count || 0)
      }
    }
    const rowGroups = Array.from(rowGroupMap.values())
    const stat = await fs.stat(parquetPath)
    const columns = {}
    columnNames.forEach((column, index) => {
      columns[column] = {
        nullCount: Number(fileStats[`null_${index}`]),
        min: fileStats[`min_${index}`],
        max: fileStats[`max_${index}`],
        approximateDistinct: Number(fileStats[`distinct_${index}`])
      }
    })

    return {
      byteSize: stat.size,
      stats: {
        rowCount: Number(fileStats.row_count),
        rowGroups,
        columns
      }
    }
  } finally {
    connection.closeSync()
    await fs.rm(tempDir, { recursive: true, force: true })
  }
}

module.exports = { convertCsvFileToParquet, quoteIdentifier, quoteLiteral }
