'use strict'

/**
 * minioClient.js
 * Downloads a partition CSV from MinIO to a temp file on disk.
 * Using a temp file avoids backpressure issues when piping large objects
 * directly from MinIO stream into csv-parse.
 */

const fs   = require('fs')
const path = require('path')
const { pipeline } = require('stream/promises')
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3')

const s3Client = new S3Client({
  endpoint: `http://${process.env.MINIO_ENDPOINT || 'minio'}:${process.env.MINIO_PORT || '9000'}`,
  region: 'us-east-1',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.MINIO_ACCESS_KEY || 'minioadmin',
    secretAccessKey: process.env.MINIO_SECRET_KEY || 'minioadmin'
  }
})

const PARTITIONS_BUCKET = 'partitions'

/**
 * Download a partition from MinIO to /tmp/{taskId}.csv
 * Returns the local file path. Caller is responsible for deleting it.
 *
 * @param {string} objectPath  - MinIO object key, e.g. "uuid/partition-0.csv"
 * @param {string} taskId      - used to create a unique temp filename
 * @returns {Promise<string>}  - absolute path to the downloaded temp file
 */
async function downloadPartitionToFile (objectPath, taskId, extension = 'csv') {
  const localPath = path.join('/tmp', `${taskId}.${extension}`)

  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: PARTITIONS_BUCKET,
      Key: objectPath
    }))
    await pipeline(response.Body, fs.createWriteStream(localPath))
    return localPath
  } catch (err) {
    fs.unlink(localPath, () => {})
    throw err
  }
}

async function uploadPartitionFile (objectPath, localPath, contentType = 'application/vnd.apache.parquet') {
  const stat = await fs.promises.stat(localPath)
  await s3Client.send(new PutObjectCommand({
    Bucket: PARTITIONS_BUCKET,
    Key: objectPath,
    Body: fs.createReadStream(localPath),
    ContentLength: stat.size,
    ContentType: contentType
  }))
  return stat.size
}

/**
 * Delete temp file after processing — fire and forget.
 */
function cleanupTempFile (localPath) {
  fs.unlink(localPath, (err) => {
    if (err && err.code !== 'ENOENT') {
      console.warn(`[MinIO] Failed to delete temp file ${localPath}:`, err.message)
    }
  })
}

module.exports = { downloadPartitionToFile, uploadPartitionFile, cleanupTempFile }
