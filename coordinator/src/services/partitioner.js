'use strict'

/**
 * partitioner.js
 * Streams an uploaded CSV from disk, splits it into N equal partitions,
 * uploads file streams to object storage,
 * and records metadata in PostgreSQL.
 *
 * Key change vs v1: two-pass streaming approach
 *  Pass 1 — count rows + infer schema (low memory: only samples first 100 rows)
 *  Pass 2 — stream rows into N temporary partition files with backpressure
 *
 * Peak memory is bounded by parser/stringifier buffers, not dataset size.
 */

const { parse }      = require('csv-parse')
const { stringify }  = require('csv-stringify')
const fs             = require('fs')
const fsPromises     = require('fs/promises')
const os             = require('os')
const path           = require('path')
const crypto         = require('crypto')
const { once }       = require('events')
const { finished }   = require('stream/promises')
const { v4: uuidv4 } = require('uuid')
const {
  S3Client,
  HeadBucketCommand,
  CreateBucketCommand,
  PutObjectCommand
} = require('@aws-sdk/client-s3')
const db             = require('../db')
const { inferType, partitionIndexForRow } = require('./partitionLogic')
const { convertCsvFileToParquet } = require('./columnarWriter')

// ── S3 client ─────────────────────────────────────────────────────────────────
const s3Client = new S3Client({
  endpoint: `http://${process.env.MINIO_ENDPOINT || 'minio'}:${process.env.MINIO_PORT || '9000'}`,
  region: 'us-east-1',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.MINIO_ACCESS_KEY || 'minioadmin',
    secretAccessKey: process.env.MINIO_SECRET_KEY || 'minioadmin'
  }
})

const DATASETS_BUCKET   = 'datasets'
const PARTITIONS_BUCKET = 'partitions'

// ── Bucket initialisation ─────────────────────────────────────────────────────
async function initBuckets () {
  for (const bucket of [DATASETS_BUCKET, PARTITIONS_BUCKET]) {
    try {
      await s3Client.send(new HeadBucketCommand({ Bucket: bucket }))
    } catch (err) {
      if (![404, 'NotFound', 'NoSuchBucket'].includes(err.$metadata?.httpStatusCode) &&
          !['NotFound', 'NoSuchBucket'].includes(err.name)) throw err
      await s3Client.send(new CreateBucketCommand({ Bucket: bucket }))
      console.log(`[MinIO] Created bucket: ${bucket}`)
    }
  }
}

// ── Pass 1: count rows + collect schema sample ────────────────────────────────
function scanCsv (filePath) {
  return new Promise((resolve, reject) => {
    let rowCount    = 0
    let columnNames = null
    const sample    = []          // first 100 rows for type inference
    const digest = crypto.createHash('sha256')

    const parser = parse({ columns: true, skip_empty_lines: true })
    parser.on('data', (row) => {
      rowCount++
      if (!columnNames) columnNames = Object.keys(row)
      if (sample.length < 100) sample.push(row)
    })
    parser.on('end',   () => resolve({ rowCount, columnNames, sample, contentChecksum: digest.digest('hex') }))
    parser.on('error', reject)

    const source = fs.createReadStream(filePath)
    source.on('data', chunk => digest.update(chunk))
    source.on('error', reject)
    source.pipe(parser)
  })
}

// ── Pass 2: stream rows into partitions, upload each to MinIO ─────────────────
/**
 * Streams through the CSV once. Rows are distributed into bounded CSV
 * stringifiers backed by temporary files. Those files are then uploaded and
 * converted one at a time, keeping heap usage independent of input size.
 */
async function streamPartitions (sourcePath, datasetId, columnNames, rowCount, partitionCount) {
  const spoolDirectory = await fsPromises.mkdtemp(path.join(os.tmpdir(), `queryforge-partitions-${datasetId}-`))
  const outputs = Array.from({ length: partitionCount }, (_, index) => {
    const csvPath = path.join(spoolDirectory, `partition-${index}.csv`)
    const file = fs.createWriteStream(csvPath)
    const csv = stringify({ header: true, columns: columnNames })
    csv.pipe(file)
    return { csvPath, file, csv, rowCount: 0 }
  })

  try {
    const parser = fs.createReadStream(sourcePath).pipe(parse({ columns: true, skip_empty_lines: true }))
    let globalIndex = 0
    for await (const row of parser) {
      const output = outputs[partitionIndexForRow(globalIndex, rowCount, partitionCount)]
      output.rowCount++
      globalIndex++
      if (!output.csv.write(row)) await once(output.csv, 'drain')
    }

    for (const output of outputs) output.csv.end()
    await Promise.all(outputs.flatMap(output => [finished(output.csv), finished(output.file)]))

    const partitionMeta = []
    for (let i = 0; i < outputs.length; i++) {
      const output = outputs[i]
      if (output.rowCount === 0) continue

      const csvStat = await fsPromises.stat(output.csvPath)
      const partitionPath = `${datasetId}/partition-${i}.csv`
      await uploadFile(PARTITIONS_BUCKET, partitionPath, output.csvPath, csvStat.size, 'text/csv')

      const localParquetPath = path.join(spoolDirectory, `partition-${i}.parquet`)
      const parquet = await convertCsvFileToParquet(
        output.csvPath,
        localParquetPath,
        columnNames,
        `${datasetId}-${i}`
      )
      const parquetPath = `${datasetId}/partition-${i}.parquet`
      await uploadFile(
        PARTITIONS_BUCKET,
        parquetPath,
        localParquetPath,
        parquet.byteSize,
        'application/vnd.apache.parquet'
      )

      partitionMeta.push({
        partitionIndex: i,
        minioPath: partitionPath,
        parquetPath,
        rowCount: output.rowCount,
        csvByteSize: csvStat.size,
        parquetByteSize: parquet.byteSize,
        stats: parquet.stats
      })
      console.log(`[Partitioner] Uploaded partition ${i}: ${output.rowCount} rows; CSV ${csvStat.size} B → Parquet ${parquet.byteSize} B`)
      await Promise.all([
        fsPromises.unlink(output.csvPath).catch(() => {}),
        fsPromises.unlink(localParquetPath).catch(() => {})
      ])
    }
    return partitionMeta
  } finally {
    for (const output of outputs) {
      if (!output.csv.destroyed) output.csv.destroy()
      if (!output.file.destroyed) output.file.destroy()
    }
    await fsPromises.rm(spoolDirectory, { recursive: true, force: true })
  }
}

async function uploadFile (bucket, key, filePath, contentLength, contentType) {
  await s3Client.send(new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fs.createReadStream(filePath),
    ContentLength: contentLength,
    ContentType: contentType
  }))
}

// ── Main entry point ──────────────────────────────────────────────────────────
async function partitionAndStore (sourcePath, originalName, partitionCount = 3) {
  const datasetId = uuidv4()
  const baseName  = originalName.replace(/[^a-z0-9_.-]/gi, '_')

  const sourceStat = await fsPromises.stat(sourcePath)
  console.log(`[Partitioner] Starting: ${(sourceStat.size / 1024 / 1024).toFixed(1)} MB CSV → ${partitionCount} partitions`)

  // ── 1. Upload raw file to MinIO ────────────────────────────────────────────
  const rawMinioPath = `${datasetId}/${baseName}`
  await uploadFile(DATASETS_BUCKET, rawMinioPath, sourcePath, sourceStat.size, 'text/csv')

  // ── 2. Pass 1: count rows + sample schema ──────────────────────────────────
  const { rowCount, columnNames, sample, contentChecksum } = await scanCsv(sourcePath)
  if (rowCount === 0) throw new Error('CSV file is empty or has no data rows')

  const schema = {
    columns: columnNames.map(name => ({ name, type: inferType(sample, name) }))
  }

  console.log(`[Partitioner] Scanned: ${rowCount.toLocaleString()} rows, ${columnNames.length} columns`)

  // ── 3. Pass 2: stream-partition + upload ───────────────────────────────────
  const partitionMeta = await streamPartitions(sourcePath, datasetId, columnNames, rowCount, partitionCount)

  // ── 4. Persist metadata to PostgreSQL ─────────────────────────────────────
  const client = await db.getClient()
  try {
    await client.query('BEGIN')

    await client.query(
      `INSERT INTO datasets
         (id, name, original_filename, minio_path, schema_json, row_count,
          partition_count, storage_format, columnar_committed_at, content_checksum,
          snapshot_version, generator_config_json)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'parquet', NOW(), $8, 1, $9)`,
      [
        datasetId,
        baseName.replace(/\.[^/.]+$/, ''),
        originalName,
        rawMinioPath,
        JSON.stringify(schema),
        rowCount,
        partitionMeta.length,
        contentChecksum,
        JSON.stringify({ source: 'csv-upload', partitionCount, originalName })
      ]
    )

    const partitionIds = []
    for (const pm of partitionMeta) {
      const partRes = await client.query(
        `INSERT INTO partitions
           (dataset_id, partition_index, minio_path, parquet_path, row_count,
            csv_byte_size, parquet_byte_size, stats_json)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
        [datasetId, pm.partitionIndex, pm.minioPath, pm.parquetPath, pm.rowCount,
          pm.csvByteSize, pm.parquetByteSize, JSON.stringify(pm.stats)]
      )
      partitionIds.push(partRes.rows[0].id)
    }

    const sourceNode = await client.query(
      `INSERT INTO lineage_nodes (kind, operator, dataset_id, metadata_json)
       VALUES ('source', 'DATASET_SOURCE', $1, $2)
       ON CONFLICT (dataset_id, operator) WHERE kind = 'source'
       DO UPDATE SET metadata_json = EXCLUDED.metadata_json
       RETURNING id`,
      [datasetId, JSON.stringify({ snapshotVersion: 1, contentChecksum, rowCount })]
    )
    for (let index = 0; index < partitionIds.length; index++) {
      const partitionNode = await client.query(
        `INSERT INTO lineage_nodes
           (kind, operator, dataset_id, partition_id, logical_partition_key, metadata_json)
         VALUES ('partition', 'SOURCE_PARTITION', $1, $2, $3, $4) RETURNING id`,
        [datasetId, partitionIds[index], `source-${index}`, JSON.stringify({ partitionIndex: index })]
      )
      await client.query(
        `INSERT INTO lineage_edges (parent_id, child_id, edge_type)
         VALUES ($1, $2, 'contains') ON CONFLICT DO NOTHING`,
        [sourceNode.rows[0].id, partitionNode.rows[0].id]
      )
    }

    await client.query('COMMIT')
    console.log(`[Partitioner] Done: dataset ${datasetId} — ${rowCount.toLocaleString()} rows → ${partitionMeta.length} partitions`)

    return { datasetId, snapshotId: datasetId, contentChecksum, rowCount, schema, partitionCount: partitionMeta.length, partitionIds }
  } catch (err) {
    await client.query('ROLLBACK')
    throw err
  } finally {
    client.release()
  }
}

module.exports = { initBuckets, partitionAndStore, scanCsv, streamPartitions, uploadFile, s3Client, PARTITIONS_BUCKET }
