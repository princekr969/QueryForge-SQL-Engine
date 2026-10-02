'use strict'

const fs = require('fs/promises')
const syncFs = require('fs')
const path = require('path')
const { DuckDBInstance } = require('@duckdb/node-api')
const { uploadPartitionFile } = require('./minioClient')
const sharedSketches = syncFs.existsSync('/shared/sketches.js') ? '/shared/sketches' : path.join(__dirname, '../../../shared/sketches')
const { DuckDBScalarFunction, VARCHAR, BOOLEAN } = require('@duckdb/node-api')
const { BloomFilter } = require(sharedSketches)

function quoteIdentifier (value) { return `"${String(value).replace(/"/g, '""')}"` }
function quoteLiteral (value) { return `'${String(value).replace(/'/g, "''")}'` }

async function executeShuffleTask (localPath, request, onResult) {
  const spec = JSON.parse(request.shuffle_spec_json)
  if (!Number.isInteger(spec.buckets) || spec.buckets < 1 || spec.buckets > 64) throw new Error('Invalid shuffle bucket count')
  const directory = `/tmp/queryforge-shuffle-${request.task_id}`
  await fs.mkdir(directory, { recursive: true })
  const instance = await DuckDBInstance.create(':memory:', {
    memory_limit: process.env.WORKER_MEMORY_LIMIT || '256MB',
    temp_directory: `${directory}/spill`,
    threads: process.env.WORKER_THREADS || '2'
  })
  const connection = await instance.connect()
  const paths = []
  const bucketRows = []
  const bucketBytes = []
  let bytesWritten = 0
  try {
    let bloomPredicate = ''
    if (spec.bloom) {
      const bloom = BloomFilter.fromJSON(spec.bloom)
      connection.registerScalarFunction(DuckDBScalarFunction.create({
        name: 'bloom_might_contain',
        mainFunction: (_info, input, output) => {
          const values = input.getColumnVector(0)
          for (let row = 0; row < input.rowCount; row++) output.setItem(row, bloom.has(values.getItem(row)))
          output.flush()
        },
        returnType: BOOLEAN,
        parameterTypes: [VARCHAR]
      }))
      bloomPredicate = ` AND bloom_might_contain(CAST(${quoteIdentifier(spec.column)} AS VARCHAR))`
    }
    for (let bucket = 0; bucket < spec.buckets; bucket++) {
      const output = `${directory}/bucket-${bucket}.parquet`
      await connection.run(`COPY (
        SELECT * FROM read_parquet(${quoteLiteral(localPath)})
        WHERE hash(${quoteIdentifier(spec.column)}) % ${spec.buckets} = ${bucket}${bloomPredicate}
      ) TO ${quoteLiteral(output)} (FORMAT PARQUET, COMPRESSION ZSTD)`)
      const objectPath = `${spec.prefix}/${spec.side}/source-${spec.sourceIndex}/bucket-${bucket}.parquet`
      const written = await uploadPartitionFile(objectPath, output)
      bytesWritten += written
      bucketBytes.push(written)
      const count = await connection.runAndReadAll(`SELECT COUNT(*) AS rows FROM read_parquet(${quoteLiteral(output)})`)
      bucketRows.push(Number(count.getRowObjectsJson()[0].rows))
      paths.push(objectPath)
    }
    onResult({
      task_id: request.task_id,
      is_aggregated: false,
      column_names: [], rows: [], groups: [], arrow_ipc: Buffer.alloc(0),
      shuffle_paths: paths,
      sketch_state: Buffer.from(JSON.stringify({ bucketRows, bucketBytes })),
      bytes_written: bytesWritten,
      is_complete: true,
      rows_scanned: Number(request.partition_row_count || 0),
      bytes_scanned: Number(request.partition_byte_size || 0),
      bytes_skipped: 0
    })
  } finally {
    connection.closeSync()
    await fs.rm(directory, { recursive: true, force: true })
  }
}

module.exports = { executeShuffleTask }
