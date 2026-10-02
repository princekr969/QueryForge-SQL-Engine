'use strict'

const fs = require('fs')
const path = require('path')
const { Kafka } = require('kafkajs')
const db = require('../db')
const { publishToChannel } = require('../websocket/wsServer')
const { workerRegistry } = require('./workerRegistry')
const { processStreamBatchOnWorker, invalidateWorkerStub } = require('../grpc/workerClient')

const sharedSketches = fs.existsSync('/shared/sketches.js') ? '/shared/sketches' : path.join(__dirname, '../../../shared/sketches')
const { HyperLogLog, FrequencySketch, sketchFromJSON } = require(sharedSketches)

const brokers = String(process.env.KAFKA_BROKERS || 'localhost:19092').split(',').map(item => item.trim()).filter(Boolean)
const kafka = new Kafka({ clientId: 'queryforge-streaming', brokers, retry: { retries: 8 } })
const consumers = new Map()
let producer = null

function intervalMs (count, unit) {
  const factors = { MILLISECOND: 1, SECOND: 1000, MINUTE: 60000, HOUR: 3600000 }
  return Number(count) * factors[String(unit).toUpperCase()]
}

function parseStandingStatement (statement) {
  if (typeof statement !== 'string') throw new Error('statement is required')
  const register = statement.match(/^\s*REGISTER\s+QUERY\s+([a-zA-Z_][\w]*)\s+AS\s+/i)
  const topic = statement.match(/\bFROM\s+([a-zA-Z_][\w.-]*)/i)
  const window = statement.match(/\b(TUMBLE|HOP|SESSION)\s*\(\s*([a-zA-Z_][\w]*)\s*,\s*INTERVAL\s+'(\d+)'\s+(MILLISECOND|SECOND|MINUTE|HOUR)(?:\s*,\s*INTERVAL\s+'(\d+)'\s+(MILLISECOND|SECOND|MINUTE|HOUR))?\s*\)/i)
  if (!register || !topic || !window) throw new Error('Expected REGISTER QUERY ... FROM topic with TUMBLE, HOP, or SESSION window')
  const type = window[1].toUpperCase()
  const firstMs = intervalMs(window[3], window[4])
  const secondMs = window[5] ? intervalMs(window[5], window[6]) : null
  if (type === 'HOP' && !secondMs) throw new Error('HOP requires slide and size intervals')
  const watermark = statement.match(/\bWATERMARK\s+INTERVAL\s+'(\d+)'\s+(MILLISECOND|SECOND|MINUTE|HOUR)/i)
  const distinct = statement.match(/COUNT\s*\(\s*DISTINCT\s+([a-zA-Z_][\w]*)\s*\)/i)
  const sum = statement.match(/SUM\s*\(\s*([a-zA-Z_][\w]*)\s*\)/i)
  return normalizeStreamConfig({
    name: register[1], topic: topic[1], eventTimeColumn: window[2], windowType: type,
    sizeMs: type === 'HOP' ? secondMs : firstMs,
    slideMs: type === 'HOP' ? firstMs : null,
    gapMs: type === 'SESSION' ? firstMs : null,
    allowedLatenessMs: watermark ? intervalMs(watermark[1], watermark[2]) : 0,
    distinctColumn: distinct?.[1] || null, sumColumn: sum?.[1] || null
  })
}

function normalizeStreamConfig (input) {
  const config = {
    name: input.name,
    topic: input.topic,
    eventTimeColumn: input.eventTimeColumn || 'ts',
    windowType: String(input.windowType || 'TUMBLE').toUpperCase(),
    sizeMs: Number(input.sizeMs || 60000),
    slideMs: input.slideMs == null ? null : Number(input.slideMs),
    gapMs: input.gapMs == null ? null : Number(input.gapMs),
    allowedLatenessMs: Number(input.allowedLatenessMs || 0),
    groupByColumn: input.groupByColumn || null,
    distinctColumn: input.distinctColumn || null,
    sumColumn: input.sumColumn || null,
    heavyHitterColumn: input.heavyHitterColumn || null,
    hllPrecision: Number(input.hllPrecision || 14),
    topK: Number(input.topK || 10),
    workerDelayMs: Math.max(0, Math.min(3000, Number(input.workerDelayMs || 0))),
    trackExactEvaluation: input.trackExactEvaluation === true
  }
  if (!config.name || !/^[a-zA-Z_][\w]*$/.test(config.name)) throw new Error('name must be a SQL identifier')
  if (!config.topic || !/^[a-zA-Z_][\w.-]*$/.test(config.topic)) throw new Error('topic is required')
  if (!['TUMBLE', 'HOP', 'SESSION'].includes(config.windowType)) throw new Error('windowType must be TUMBLE, HOP, or SESSION')
  if (!Number.isInteger(config.sizeMs) || config.sizeMs < 100 || config.sizeMs > 86400000) throw new Error('sizeMs must be from 100 to 86400000')
  if (config.windowType === 'HOP' && (!Number.isInteger(config.slideMs) || config.slideMs < 100 || config.slideMs > config.sizeMs)) throw new Error('HOP slideMs must be from 100 to sizeMs')
  if (config.windowType === 'SESSION' && (!Number.isInteger(config.gapMs) || config.gapMs < 100)) config.gapMs = config.sizeMs
  if (!Number.isInteger(config.allowedLatenessMs) || config.allowedLatenessMs < 0 || config.allowedLatenessMs > 86400000) throw new Error('allowedLatenessMs is invalid')
  if (!Number.isInteger(config.hllPrecision) || config.hllPrecision < 14 || config.hllPrecision > 16) throw new Error('hllPrecision must be 14-16 (<1% configured error)')
  return config
}

function fixedWindows (timestampMs, config) {
  if (config.windowType === 'TUMBLE') {
    const start = Math.floor(timestampMs / config.sizeMs) * config.sizeMs
    return [{ start, end: start + config.sizeMs }]
  }
  if (config.windowType === 'HOP') {
    const windows = []
    const latestStart = Math.floor(timestampMs / config.slideMs) * config.slideMs
    for (let start = latestStart; start > timestampMs - config.sizeMs; start -= config.slideMs) {
      windows.push({ start, end: start + config.sizeMs })
    }
    return windows
  }
  return []
}

function createState (config) {
  return {
    sum: 0,
    hll: config.distinctColumn ? new HyperLogLog(config.hllPrecision).toJSON() : null,
    frequency: config.heavyHitterColumn ? new FrequencySketch(2048, 5, config.topK).toJSON() : null,
    evaluation: config.trackExactEvaluation ? { distinct: [], frequency: {}, events: 0 } : null
  }
}

function mergeEventState (stored, event, config) {
  const state = stored || createState(config)
  if (config.sumColumn) state.sum += Number(event[config.sumColumn] || 0)
  if (config.distinctColumn && event[config.distinctColumn] !== undefined && event[config.distinctColumn] !== null) {
    const hll = sketchFromJSON(state.hll)
    hll.add(event[config.distinctColumn])
    state.hll = hll.toJSON()
  }
  if (config.heavyHitterColumn && event[config.heavyHitterColumn] !== undefined) {
    const frequency = sketchFromJSON(state.frequency)
    frequency.add(event[config.heavyHitterColumn])
    state.frequency = frequency.toJSON()
  }
  if (state.evaluation) {
    state.evaluation.events++
    if (config.distinctColumn && event[config.distinctColumn] !== undefined && state.evaluation.distinct.length < 50000) {
      const value = String(event[config.distinctColumn])
      if (!state.evaluation.distinct.includes(value)) state.evaluation.distinct.push(value)
    }
    if (config.heavyHitterColumn && event[config.heavyHitterColumn] !== undefined) {
      const value = String(event[config.heavyHitterColumn])
      state.evaluation.frequency[value] = Number(state.evaluation.frequency[value] || 0) + 1
    }
  }
  return state
}

function mergeStates (left, right, config) {
  const merged = left || createState(config)
  merged.sum += Number(right?.sum || 0)
  if (config.distinctColumn && right?.hll) {
    const sketch = sketchFromJSON(merged.hll); sketch.merge(sketchFromJSON(right.hll)); merged.hll = sketch.toJSON()
  }
  if (config.heavyHitterColumn && right?.frequency) {
    const sketch = sketchFromJSON(merged.frequency); sketch.merge(sketchFromJSON(right.frequency)); merged.frequency = sketch.toJSON()
  }
  if (merged.evaluation && right?.evaluation) {
    merged.evaluation.events += Number(right.evaluation.events || 0)
    merged.evaluation.distinct = [...new Set([...merged.evaluation.distinct, ...(right.evaluation.distinct || [])])].slice(0, 50000)
    for (const [value, count] of Object.entries(right.evaluation.frequency || {})) {
      merged.evaluation.frequency[value] = Number(merged.evaluation.frequency[value] || 0) + Number(count)
    }
  }
  return merged
}

function stateResult (state, config) {
  const hll = state.hll ? sketchFromJSON(state.hll) : null
  const frequency = state.frequency ? sketchFromJSON(state.frequency) : null
  const distinctUsers = hll ? hll.estimate() : null
  const topItems = frequency ? frequency.topK(config.topK) : []
  const exactDistinct = state.evaluation?.distinct?.length ?? null
  const exactFrequency = state.evaluation?.frequency || null
  const frequencyObservedError = exactFrequency && state.evaluation.events > 0
    ? Math.max(0, ...Object.entries(exactFrequency).map(([value, count]) =>
        Math.abs(frequency.estimate(value) - Number(count)) / state.evaluation.events))
    : null
  return {
    revenue: Number(state.sum || 0),
    distinctUsers,
    distinctConfiguredError: hll ? 1.04 / Math.sqrt(2 ** config.hllPrecision) : null,
    distinctExact: exactDistinct,
    distinctObservedError: exactDistinct > 0 ? Math.abs(distinctUsers - exactDistinct) / exactDistinct : null,
    topItems,
    frequencyConfiguredError: frequency ? Math.E / frequency.width : null,
    frequencyFailureProbability: frequency ? Math.exp(-frequency.depth) : null,
    frequencyObservedError,
    exactTopItems: exactFrequency
      ? Object.entries(exactFrequency).map(([value, count]) => ({ value, count }))
          .sort((left, right) => right.count - left.count || left.value.localeCompare(right.value)).slice(0, config.topK)
      : null
  }
}

async function upsertFixedWindow (client, queryId, event, eventTime, config, assignedWindows = null) {
  const groupKey = config.groupByColumn ? String(event[config.groupByColumn] ?? '') : ''
  for (const window of assignedWindows || fixedWindows(eventTime, config)) {
    const key = `${window.start}:${window.end}:${groupKey}`
    const existing = await client.query(
      'SELECT state_json, event_count FROM stream_windows WHERE query_id = $1 AND window_key = $2 FOR UPDATE', [queryId, key]
    )
    const state = mergeEventState(existing.rows[0]?.state_json || null, event, config)
    await client.query(
      `INSERT INTO stream_windows
         (query_id, window_key, window_start, window_end, group_key, state_json, event_count)
       VALUES ($1,$2,$3,$4,$5,$6,1)
       ON CONFLICT (query_id, window_key) DO UPDATE SET
         state_json = EXCLUDED.state_json, event_count = stream_windows.event_count + 1,
         updated_at = NOW()`,
      [queryId, key, new Date(window.start), new Date(window.end), groupKey, JSON.stringify(state)]
    )
  }
}

function sessionMatchBounds (eventTime, gapMs) {
  return {
    latestStart: eventTime + gapMs,
    earliestEnd: eventTime
  }
}

function isTooLate (eventTime, watermark) {
  return watermark > 0 && eventTime <= watermark
}

async function prepareMicroBatch (record, batch, messages) {
  const events = messages.map(message => JSON.parse(message.value.toString('utf8')))
  const request = {
    batch_id: `${record.id}:${batch.topic}:${batch.partition}:${messages[0]?.offset || 'empty'}:${messages.at(-1)?.offset || 'empty'}`,
    config_json: JSON.stringify(record.config_json),
    events_json: JSON.stringify(events)
  }
  const attempted = new Set()
  const deadline = Date.now() + 15000
  let lastError = null
  while (Date.now() < deadline) {
    const workers = [...workerRegistry.values()]
      .filter(worker => worker.status === 'active' && worker.capabilities?.includes('stream'))
      .filter(worker => !attempted.has(`${worker.address}:${worker.port}`))
      .sort((left, right) => left.activeTasks - right.activeTasks || left.workerId.localeCompare(right.workerId))
    if (workers.length === 0) {
      if (attempted.size > 0) break
      await new Promise(resolve => setTimeout(resolve, 250))
      continue
    }
    const worker = workers[0]
    const address = `${worker.address}:${worker.port}`
    attempted.add(address)
    try {
      const records = await processStreamBatchOnWorker(address, request)
      if (records.length !== messages.length) throw new Error('Worker changed stream micro-batch cardinality')
      return { records, workerId: worker.workerId }
    } catch (error) {
      lastError = error
      invalidateWorkerStub(address)
      console.warn(`[Streaming] Micro-batch worker ${worker.workerId} failed; retrying elsewhere: ${error.message}`)
    }
  }
  throw new Error(`No stream-capable worker completed micro-batch: ${lastError?.message || 'none registered'}`)
}

async function upsertSessionWindow (client, queryId, event, eventTime, config) {
  const groupKey = config.groupByColumn ? String(event[config.groupByColumn] ?? '') : ''
  const bounds = sessionMatchBounds(eventTime, config.gapMs)
  const matches = await client.query(
    `SELECT * FROM stream_windows WHERE query_id = $1 AND group_key = $2 AND status = 'open'
     AND window_start <= $3 AND window_end >= $4 FOR UPDATE`,
    [queryId, groupKey, new Date(bounds.latestStart), new Date(bounds.earliestEnd)]
  )
  let start = eventTime
  let end = eventTime + config.gapMs
  let state = null
  let count = 1
  for (const match of matches.rows) {
    start = Math.min(start, new Date(match.window_start).getTime())
    end = Math.max(end, new Date(match.window_end).getTime(), eventTime + config.gapMs)
    state = mergeStates(state, match.state_json, config)
    count += Number(match.event_count)
  }
  state = mergeEventState(state, event, config)
  if (matches.rowCount) await client.query('DELETE FROM stream_windows WHERE query_id = $1 AND window_key = ANY($2::text[])', [queryId, matches.rows.map(item => item.window_key)])
  const key = `${start}:${end}:${groupKey}`
  await client.query(
    `INSERT INTO stream_windows
       (query_id, window_key, window_start, window_end, group_key, state_json, event_count)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [queryId, key, new Date(start), new Date(end), groupKey, JSON.stringify(state), count]
  )
}

async function processBatch (record, batch) {
  const config = record.config_json
  const client = await db.getClient()
  let nextOffset = null
  let committedEvents = 0
  let acceptedLateCount = 0
  let auditedLateCount = 0
  let windowAssignments = 0
  let revenueDelta = 0
  let durableNext = 0n
  try {
    await client.query('BEGIN')
    const durable = await client.query(
      'SELECT next_offset FROM stream_offsets WHERE query_id=$1 AND topic=$2 AND partition=$3 FOR UPDATE',
      [record.id, batch.topic, batch.partition]
    )
    durableNext = BigInt(durable.rows[0]?.next_offset || 0)
    let maxEventTime = record.max_event_time ? new Date(record.max_event_time).getTime() : 0
    let watermark = record.watermark ? new Date(record.watermark).getTime() : 0
    const fresh = batch.messages.filter(message => BigInt(message.offset) >= durableNext)
    const prepared = fresh.length ? await prepareMicroBatch(record, batch, fresh) : { records: [], workerId: null }
    for (let index = 0; index < fresh.length; index++) {
      const message = fresh[index]
      const { event, eventTime, windows } = prepared.records[index]
      const behindMaximum = maxEventTime > 0 && eventTime < maxEventTime
      if (isTooLate(eventTime, watermark)) {
        auditedLateCount++
        await client.query(
          `INSERT INTO stream_late_events
             (query_id,topic,partition,event_offset,event_time,watermark,payload_json)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
          [record.id, batch.topic, batch.partition, message.offset, new Date(eventTime), new Date(watermark), JSON.stringify(event)]
        )
      } else if (config.windowType === 'SESSION') {
        if (behindMaximum) acceptedLateCount++
        windowAssignments++
        revenueDelta += Number(config.sumColumn ? event[config.sumColumn] || 0 : 0)
        await upsertSessionWindow(client, record.id, event, eventTime, config)
      } else {
        if (behindMaximum) acceptedLateCount++
        windowAssignments += windows.length
        revenueDelta += Number(config.sumColumn ? event[config.sumColumn] || 0 : 0) * windows.length
        await upsertFixedWindow(client, record.id, event, eventTime, config, windows)
      }
      maxEventTime = Math.max(maxEventTime, eventTime)
      watermark = maxEventTime - config.allowedLatenessMs
      nextOffset = (BigInt(message.offset) + 1n).toString()
      committedEvents++
    }
    if (nextOffset !== null) {
      await client.query(
        `INSERT INTO stream_offsets (query_id,topic,partition,next_offset) VALUES ($1,$2,$3,$4)
         ON CONFLICT (query_id,topic,partition) DO UPDATE SET next_offset=EXCLUDED.next_offset,updated_at=NOW()`,
        [record.id, batch.topic, batch.partition, nextOffset]
      )
      await client.query(
        `INSERT INTO stream_batches
           (query_id,topic,partition,first_offset,next_offset,event_count,worker_id,
            output_epoch,accepted_late_count,audited_late_count,accumulator_delta_json)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`,
        [record.id, batch.topic, batch.partition, fresh[0].offset, nextOffset, committedEvents,
          prepared.workerId, nextOffset, acceptedLateCount, auditedLateCount,
          JSON.stringify({ inputEvents: committedEvents, windowAssignments, revenueDelta })]
      )
      await client.query(
        `UPDATE stream_queries SET max_event_time=$2,watermark=$3,updated_at=NOW() WHERE id=$1`,
        [record.id, new Date(maxEventTime), new Date(watermark)]
      )
      await client.query(
        `UPDATE stream_windows SET status='finalized',updated_at=NOW()
         WHERE query_id=$1 AND status='open' AND window_end <= $2`, [record.id, new Date(watermark)]
      )
    }
    await client.query('COMMIT')
    record.max_event_time = maxEventTime ? new Date(maxEventTime) : null
    record.watermark = watermark ? new Date(watermark) : null
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally { client.release() }
  return { nextOffset: nextOffset ?? durableNext.toString(), committedEvents }
}

async function snapshot (queryId) {
  const client = await db.getClient()
  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
    const query = await client.query('SELECT * FROM stream_queries WHERE id=$1', [queryId])
    if (query.rowCount === 0) { await client.query('ROLLBACK'); return null }
    const config = query.rows[0].config_json
    const [windows, late, offsets, batches, lateness] = await Promise.all([
      client.query('SELECT * FROM stream_windows WHERE query_id=$1 ORDER BY window_start,group_key', [queryId]),
      client.query('SELECT COUNT(*)::int AS count FROM stream_late_events WHERE query_id=$1', [queryId]),
      client.query('SELECT * FROM stream_offsets WHERE query_id=$1 ORDER BY partition', [queryId]),
      client.query('SELECT * FROM stream_batches WHERE query_id=$1 ORDER BY committed_at DESC LIMIT 100', [queryId]),
      client.query('SELECT COALESCE(SUM(accepted_late_count),0)::int AS accepted FROM stream_batches WHERE query_id=$1', [queryId])
    ])
    await client.query('COMMIT')
    return {
      query: query.rows[0], lateEvents: late.rows[0].count,
      acceptedLateEvents: lateness.rows[0].accepted,
      offsets: offsets.rows, batches: batches.rows,
      outputEpoch: Object.fromEntries(offsets.rows.map(offset => [String(offset.partition), String(offset.next_offset)])),
      windows: windows.rows.map(item => {
        const { state_json: state, ...publicWindow } = item
        return { ...publicWindow, result: stateResult(state, config) }
      })
    }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  } finally { client.release() }
}

async function startStream (record) {
  if (consumers.has(record.id)) return
  const consumer = kafka.consumer({ groupId: `queryforge-${record.id}` })
  await consumer.connect()
  await consumer.subscribe({ topic: record.source_topic, fromBeginning: true })
  consumers.set(record.id, consumer)
  consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const outcome = await processBatch(record, batch)
      for (const message of batch.messages) resolveOffset(message.offset)
      if (outcome.nextOffset !== null) {
        await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: outcome.nextOffset }])
        const current = await snapshot(record.id)
        publishToChannel(record.id, { type: 'stream_update', committedEvents: outcome.committedEvents, ...current })
      }
      await heartbeat()
    }
  }).catch(error => {
    consumers.delete(record.id)
    console.error(`[Streaming] ${record.name} stopped:`, error.message)
  })
}

async function stopStream (queryId) {
  const consumer = consumers.get(queryId)
  if (consumer) { consumers.delete(queryId); await consumer.disconnect() }
}

async function resumeStreams () {
  const active = await db.query("SELECT * FROM stream_queries WHERE status='active'")
  for (const record of active.rows) {
    try { await startStream(record) } catch (error) { console.warn(`[Streaming] Could not resume ${record.name}: ${error.message}`) }
  }
}

async function publishEvents (topic, events) {
  if (!producer) { producer = kafka.producer(); await producer.connect() }
  await producer.send({ topic, messages: events.map(event => ({ value: JSON.stringify(event) })) })
  return events.length
}

async function shutdownStreaming () {
  await Promise.all([...consumers.values()].map(consumer => consumer.disconnect().catch(() => {})))
  consumers.clear()
  if (producer) await producer.disconnect().catch(() => {})
}

module.exports = {
  parseStandingStatement, normalizeStreamConfig, fixedWindows, stateResult,
  createState, mergeEventState, mergeStates, sessionMatchBounds, isTooLate,
  startStream, stopStream, resumeStreams, publishEvents, snapshot, shutdownStreaming
}
