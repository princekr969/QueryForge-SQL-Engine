'use strict'

const crypto = require('crypto')
const fs = require('fs/promises')
const path = require('path')
const { downloadPartitionToFile, cleanupTempFile } = require('./minioClient')

const CACHE_DIRECTORY = '/tmp/queryforge-worker-cache'
const entries = new Map()
const pending = new Map()
let totalBytes = 0
let mutationQueue = Promise.resolve()
const counters = { hits: 0, misses: 0, evictions: 0, bypasses: 0 }

function withMutationLock (operation) {
  const next = mutationQueue.then(operation, operation)
  mutationQueue = next.catch(() => {})
  return next
}

function normalizedLevel (value) {
  const level = String(value || 'NONE').toUpperCase()
  return ['NONE', 'MEMORY', 'DISK', 'MEMORY_AND_DISK'].includes(level) ? level : 'NONE'
}

async function evictFor (additionalBytes, budgetBytes) {
  const candidates = [...entries.entries()].filter(([, entry]) => entry.refs === 0)
    .sort((left, right) => left[1].lastUsed - right[1].lastUsed)
  while (totalBytes + additionalBytes > budgetBytes && candidates.length) {
    const [key, entry] = candidates.shift()
    entries.delete(key)
    totalBytes -= entry.size
    counters.evictions++
    if (entry.path) await fs.rm(entry.path, { force: true })
  }
}

async function acquireCachedPartition (objectPath, taskId, extension, levelValue, budgetValue) {
  const requestedLevel = normalizedLevel(levelValue)
  const budgetBytes = Math.max(1024 * 1024, Number(budgetValue || 256 * 1024 * 1024))
  if (requestedLevel === 'NONE') {
    counters.bypasses++
    const localPath = await downloadPartitionToFile(objectPath, taskId, extension)
    return { path: localPath, cacheHit: false, cacheLevel: 'NONE', size: (await fs.stat(localPath)).size,
      release: async () => cleanupTempFile(localPath) }
  }

  await fs.mkdir(CACHE_DIRECTORY, { recursive: true })
  const key = `${requestedLevel}:${objectPath}`
  if (pending.has(key)) await pending.get(key)
  let entry = entries.get(key)
  let cacheHit = Boolean(entry)
  if (cacheHit) counters.hits++
  else counters.misses++
  if (!entry) {
    const load = (async () => {
      const staging = await downloadPartitionToFile(objectPath, `${taskId}-cache`, extension)
      const stat = await fs.stat(staging)
      return withMutationLock(async () => {
        await evictFor(stat.size > budgetBytes ? 0 : stat.size, budgetBytes)
        if (stat.size > budgetBytes || totalBytes + stat.size > budgetBytes) {
          counters.bypasses++
          return { transientPath: staging, size: stat.size }
        }
        const resolvedLevel = requestedLevel === 'MEMORY_AND_DISK'
          ? (stat.size <= budgetBytes / 4 ? 'MEMORY' : 'DISK')
          : requestedLevel
        const created = { level: resolvedLevel, size: stat.size, refs: 0, lastUsed: Date.now() }
        if (resolvedLevel === 'MEMORY') {
          created.data = await fs.readFile(staging)
          cleanupTempFile(staging)
        } else {
          created.path = path.join(CACHE_DIRECTORY, `${crypto.createHash('sha256').update(key).digest('hex')}.${extension}`)
          await fs.rename(staging, created.path)
        }
        entries.set(key, created)
        totalBytes += created.size
        return created
      })
    })()
    pending.set(key, load)
    try { entry = await load } finally { pending.delete(key) }
  }
  if (entry.transientPath) {
    return { path: entry.transientPath, cacheHit: false, cacheLevel: 'NONE', size: entry.size,
      release: async () => cleanupTempFile(entry.transientPath) }
  }
  entry.refs++
  entry.lastUsed = Date.now()
  if (entry.level === 'MEMORY') {
    const localPath = path.join('/tmp', `${taskId}.${extension}`)
    await fs.writeFile(localPath, entry.data)
    return { path: localPath, cacheHit, cacheLevel: 'MEMORY', size: entry.size,
      release: async () => {
        entry.refs--
        cleanupTempFile(localPath)
        await withMutationLock(() => evictFor(0, budgetBytes))
      } }
  }
  return { path: entry.path, cacheHit, cacheLevel: 'DISK', size: entry.size,
    release: async () => {
      entry.refs--
      entry.lastUsed = Date.now()
      await withMutationLock(() => evictFor(0, budgetBytes))
    } }
}

function snapshot () {
  return {
    ...counters,
    entries: entries.size,
    totalBytes,
    values: [...entries.entries()].map(([key, value]) => ({
      key, level: value.level, size: value.size, refs: value.refs, lastUsed: value.lastUsed
    }))
  }
}

module.exports = { acquireCachedPartition, normalizedLevel, snapshot }
