'use strict'

const { WebSocketServer } = require('ws')
const db = require('../db')

// Map of jobId → Set<WebSocket>
const jobSubscribers = new Map()

let wssInstance = null

function attachWebSocketServer (httpServer) {
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' })
  wssInstance = wss

  // ── Ping/pong keepalive — terminate stale connections every 30 s ─────────────
  const pingInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        ws.terminate()
        return
      }
      ws.isAlive = false
      ws.ping()
    })
  }, 30_000)

  wss.on('close', () => clearInterval(pingInterval))

  wss.on('connection', (ws) => {
    ws.isAlive = true
    ws.on('pong', () => { ws.isAlive = true })

    let subscribedJobId = null

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString())

        if (msg.type === 'subscribe' && msg.jobId) {
          subscribedJobId = msg.jobId
          ws.queryForgeReplayPending = true
          ws.queryForgePendingEvents = []
          let terminalReplay = false

          if (!jobSubscribers.has(subscribedJobId)) {
            jobSubscribers.set(subscribedJobId, new Set())
          }
          jobSubscribers.get(subscribedJobId).add(ws)

          console.log(`[WS] Client subscribed to job ${subscribedJobId}`)
          ws.send(JSON.stringify({ type: 'subscribed', jobId: subscribedJobId }))

          // A completed job is a commit marker: replay its durable rows before
          // sending complete, preserving the same event order as live execution.
          try {
            const result = await db.query(
              `SELECT status, result_row_count, execution_time_ms FROM jobs WHERE id = $1`,
              [subscribedJobId]
            )
            if (result.rows.length > 0 && result.rows[0].status === 'completed') {
              terminalReplay = true
              const persistedRows = await db.query(
                `SELECT row_data FROM job_result_rows
                 WHERE job_id = $1 ORDER BY row_index`,
                [subscribedJobId]
              )
              for (const row of persistedRows.rows) {
                if (ws.readyState !== 1) break
                ws.send(JSON.stringify({ type: 'row', data: row.row_data }))
              }
              ws.send(JSON.stringify({
                type:           'complete',
                totalRows:      result.rows[0].result_row_count || 0,
                executionTimeMs: result.rows[0].execution_time_ms || 0
              }))
              jobSubscribers.get(subscribedJobId)?.delete(ws)
            } else if (result.rows.length > 0 && result.rows[0].status === 'failed') {
              terminalReplay = true
              ws.send(JSON.stringify({ type: 'error', message: 'Job failed' }))
              jobSubscribers.get(subscribedJobId)?.delete(ws)
            }
          } catch {
            // Non-fatal — best-effort check
          } finally {
            ws.queryForgeReplayPending = false
            if (!terminalReplay && ws.readyState === 1) {
              for (const payload of ws.queryForgePendingEvents) ws.send(payload)
            }
            ws.queryForgePendingEvents = []
          }
        }
      } catch {
        try { ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' })) } catch {}
      }
    })

    ws.on('close', () => {
      if (subscribedJobId && jobSubscribers.has(subscribedJobId)) {
        jobSubscribers.get(subscribedJobId).delete(ws)
        if (jobSubscribers.get(subscribedJobId).size === 0) {
          jobSubscribers.delete(subscribedJobId)
        }
      }
    })

    ws.on('error', (err) => {
      console.error('[WS] WebSocket error:', err.message)
    })
  })

  console.log('[WS] WebSocket server attached at /ws')
  return wss
}

function getJobSubscribers (jobId) {
  return jobSubscribers.get(jobId) || new Set()
}

function publishToChannel (channelId, event) {
  const payload = JSON.stringify(event)
  for (const ws of getJobSubscribers(channelId)) {
    if (ws.readyState === 1) ws.send(payload)
  }
}

module.exports = { attachWebSocketServer, getJobSubscribers, publishToChannel }
