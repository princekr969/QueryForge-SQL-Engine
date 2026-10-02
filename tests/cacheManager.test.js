'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs/promises')
const path = require('node:path')
const { after, test } = require('node:test')

const minioModule = require.resolve('../worker/src/services/minioClient')
const cacheModule = require.resolve('../worker/src/services/cacheManager')
const cacheDirectory = '/tmp/queryforge-worker-cache'
const downloads = new Map()

require.cache[minioModule] = {
  id: minioModule,
  filename: minioModule,
  loaded: true,
  exports: {
    downloadPartitionToFile: async (objectPath, taskId, extension) => {
      downloads.set(objectPath, Number(downloads.get(objectPath) || 0) + 1)
      const localPath = path.join('/tmp', `${taskId}.${extension}`)
      const size = objectPath.startsWith('large-') ? 600 * 1024 : 64 * 1024
      await fs.writeFile(localPath, Buffer.alloc(size, objectPath.charCodeAt(0)))
      return localPath
    },
    cleanupTempFile: localPath => fs.rm(localPath, { force: true })
  }
}
delete require.cache[cacheModule]
const { acquireCachedPartition, normalizedLevel, snapshot } = require(cacheModule)

after(async () => {
  await fs.rm(cacheDirectory, { recursive: true, force: true })
})

test('cache levels fail closed and concurrent reads use one bounded load', async () => {
  assert.equal(normalizedLevel('memory'), 'MEMORY')
  assert.equal(normalizedLevel('unknown'), 'NONE')

  const before = Number(downloads.get('small-single-flight') || 0)
  const [first, second] = await Promise.all([
    acquireCachedPartition('small-single-flight', 'single-a', 'parquet', 'MEMORY', 4 * 1024 * 1024),
    acquireCachedPartition('small-single-flight', 'single-b', 'parquet', 'MEMORY', 4 * 1024 * 1024)
  ])
  assert.equal(Number(downloads.get('small-single-flight')) - before, 1)
  assert.equal([first.cacheHit, second.cacheHit].filter(Boolean).length, 1)
  assert.equal(first.cacheLevel, 'MEMORY')
  assert.equal(second.cacheLevel, 'MEMORY')
  await Promise.all([first.release(), second.release()])
  assert.ok(snapshot().totalBytes <= 4 * 1024 * 1024)
})

test('LRU eviction respects the current budget and never evicts a pinned entry', async () => {
  const budget = 1024 * 1024
  const first = await acquireCachedPartition('large-a', 'lru-a', 'parquet', 'DISK', budget)
  await first.release()
  const second = await acquireCachedPartition('large-b', 'lru-b', 'parquet', 'DISK', budget)
  await second.release()
  const afterEviction = snapshot()
  assert.ok(afterEviction.evictions >= 1)
  assert.ok(afterEviction.totalBytes <= budget)
  assert.equal(second.cacheLevel, 'DISK')

  const pinned = await acquireCachedPartition('large-b', 'pin-b', 'parquet', 'DISK', budget)
  assert.equal(pinned.cacheHit, true)
  const pressured = await acquireCachedPartition('large-c', 'pin-c', 'parquet', 'DISK', budget)
  assert.equal(pressured.cacheLevel, 'NONE')
  assert.equal(pressured.cacheHit, false)
  assert.ok(snapshot().bypasses >= 1)
  assert.ok(snapshot().totalBytes <= budget)
  assert.ok(snapshot().values.some(entry => entry.refs === 1))
  await Promise.all([pressured.release(), pinned.release()])
  assert.ok(snapshot().values.every(entry => entry.refs === 0))
})
