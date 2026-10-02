'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { processStreamBatch } = require('../worker/src/services/streamBatchExecutor')

test('stream worker assigns every HOP event to each overlapping window', () => {
  const response = processStreamBatch({
    batch_id: 'batch-1',
    config_json: JSON.stringify({ eventTimeColumn: 'ts', windowType: 'HOP', sizeMs: 3000, slideMs: 1000 }),
    events_json: JSON.stringify([{ ts: '2026-10-01T00:00:00.000Z' }, { ts: '2026-10-01T00:00:01.000Z' }])
  })
  const records = JSON.parse(response.records_json)
  assert.equal(response.event_count, 2)
  assert.deepEqual(records.map(record => record.windows.length), [3, 3])
})

test('stream worker validates event time and leaves SESSION merging to durable state', () => {
  const response = processStreamBatch({
    batch_id: 'batch-2',
    config_json: JSON.stringify({ eventTimeColumn: 'ts', windowType: 'SESSION', sizeMs: 1000 }),
    events_json: JSON.stringify([{ ts: '2026-10-01T00:00:00.000Z', user: 'a' }])
  })
  assert.deepEqual(JSON.parse(response.records_json)[0].windows, [])
  assert.throws(() => processStreamBatch({
    batch_id: 'bad',
    config_json: JSON.stringify({ eventTimeColumn: 'ts', windowType: 'TUMBLE', sizeMs: 1000 }),
    events_json: JSON.stringify([{ ts: 'not-a-date' }])
  }), /Invalid event time/)
})
