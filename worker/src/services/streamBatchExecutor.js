'use strict'

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

function processStreamBatch ({ batch_id: batchId, config_json: configJson, events_json: eventsJson }) {
  const config = JSON.parse(configJson)
  const events = JSON.parse(eventsJson)
  if (!Array.isArray(events) || events.length > 10000) throw new Error('Stream micro-batch must contain at most 10000 events')
  if (!['TUMBLE', 'HOP', 'SESSION'].includes(config.windowType)) throw new Error('Unsupported stream window type')
  const records = events.map(event => {
    const eventTime = new Date(event[config.eventTimeColumn]).getTime()
    if (!Number.isFinite(eventTime)) throw new Error(`Invalid event time in ${config.eventTimeColumn}`)
    return { event, eventTime, windows: fixedWindows(eventTime, config) }
  })
  return { batch_id: batchId, records_json: JSON.stringify(records), event_count: records.length }
}

module.exports = { fixedWindows, processStreamBatch }
