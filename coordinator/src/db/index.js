'use strict'

const { Pool } = require('pg')
const fs = require('fs')
const path = require('path')

const pool = new Pool({
  connectionString:        process.env.DATABASE_URL,
  max:                     20,
  idleTimeoutMillis:       30_000,
  connectionTimeoutMillis: 2_000
})

pool.on('error', (err) => {
  console.error('[DB] Unexpected pool error:', err.message)
})

async function query (text, params) {
  const start = Date.now()
  try {
    const result = await pool.query(text, params)
    if (process.env.NODE_ENV !== 'production') {
      console.debug(`[DB] ${Date.now() - start}ms: ${text.slice(0, 80)}`)
    }
    return result
  } catch (err) {
    console.error('[DB] Query error:', err.message, '\nSQL:', text)
    throw err
  }
}

async function getClient () {
  return pool.connect()
}

async function end () {
  return pool.end()
}

async function runMigrations () {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `)

  const migrationsDir = path.join(__dirname, '../../migrations')
  const files = fs.readdirSync(migrationsDir).filter(name => name.endsWith('.sql')).sort()

  for (const name of files) {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      const applied = await client.query('SELECT 1 FROM schema_migrations WHERE name = $1', [name])
      if (applied.rowCount === 0) {
        await client.query(fs.readFileSync(path.join(migrationsDir, name), 'utf8'))
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [name])
        console.log(`[DB] Applied migration ${name}`)
      }
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    } finally {
      client.release()
    }
  }
}

module.exports = { query, getClient, runMigrations, end }
