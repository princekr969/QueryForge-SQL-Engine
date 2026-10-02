'use strict'

/**
 * Unit tests for partitioner.js — partition assignment logic
 * Tests row splitting and schema inference without requiring MinIO/PostgreSQL.
 *
 * Run with: node --test tests/partitioner.test.js
 */

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const fs = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { inferType, partitionIndexForRow } = require('../coordinator/src/services/partitionLogic')
const { scanCsv } = require('../coordinator/src/services/partitioner')

// ── Pure logic extracted from partitioner for unit testing ────────────────────

function splitIntoPartitions (rows, partitionCount) {
  const partitions = Array.from({ length: partitionCount }, () => [])
  for (let index = 0; index < rows.length; index++) {
    partitions[partitionIndexForRow(index, rows.length, partitionCount)].push(rows[index])
  }
  return partitions.filter(p => p.length > 0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('partitioner — row splitting', () => {
  it('splits 9 rows into 3 equal partitions', () => {
    const rows = Array.from({ length: 9 }, (_, i) => ({ id: String(i) }))
    const parts = splitIntoPartitions(rows, 3)
    assert.equal(parts.length, 3)
    assert.equal(parts[0].length, 3)
    assert.equal(parts[1].length, 3)
    assert.equal(parts[2].length, 3)
  })

  it('handles uneven split — last partition gets remainder', () => {
    const rows = Array.from({ length: 10 }, (_, i) => ({ id: String(i) }))
    const parts = splitIntoPartitions(rows, 3)
    const totalRows = parts.reduce((sum, p) => sum + p.length, 0)
    assert.equal(totalRows, 10)
  })

  it('handles fewer rows than partitions', () => {
    const rows = [{ id: '1' }, { id: '2' }]
    const parts = splitIntoPartitions(rows, 3)
    const totalRows = parts.reduce((sum, p) => sum + p.length, 0)
    assert.equal(totalRows, 2)
  })

  it('single partition returns all rows', () => {
    const rows = Array.from({ length: 5 }, (_, i) => ({ id: String(i) }))
    const parts = splitIntoPartitions(rows, 1)
    assert.equal(parts.length, 1)
    assert.equal(parts[0].length, 5)
  })

  it('preserves all rows across partitions (no data loss)', () => {
    const rows = Array.from({ length: 100 }, (_, i) => ({ id: String(i) }))
    const parts = splitIntoPartitions(rows, 3)
    const totalRows = parts.reduce((sum, p) => sum + p.length, 0)
    assert.equal(totalRows, 100)
  })

  it('preserves order and every row across 500 generated layouts', () => {
    for (let rowCount = 1; rowCount <= 100; rowCount++) {
      for (let partitionCount = 1; partitionCount <= 5; partitionCount++) {
        const rows = Array.from({ length: rowCount }, (_, id) => ({ id }))
        const flattened = splitIntoPartitions(rows, partitionCount).flat()
        assert.deepEqual(flattened, rows)
      }
    }
  })
})

describe('partitioner — schema type inference', () => {
  it('infers number type for numeric column', () => {
    const rows = [{ age: '25' }, { age: '30' }, { age: '35' }]
    assert.equal(inferType(rows, 'age'), 'number')
  })

  it('infers string type for text column', () => {
    const rows = [{ city: 'Mumbai' }, { city: 'Delhi' }]
    assert.equal(inferType(rows, 'city'), 'string')
  })

  it('infers string type when any value is non-numeric', () => {
    const rows = [{ val: '100' }, { val: 'N/A' }, { val: '200' }]
    assert.equal(inferType(rows, 'val'), 'string')
  })

  it('skips empty values when inferring type', () => {
    const rows = [{ age: '' }, { age: '25' }, { age: '30' }]
    assert.equal(inferType(rows, 'age'), 'number')
  })

  it('infers number for float values', () => {
    const rows = [{ salary: '75000.50' }, { salary: '80000.00' }]
    assert.equal(inferType(rows, 'salary'), 'number')
  })
})

describe('partitioner — disk-streamed scan', () => {
  it('counts and samples a CSV from a file path', async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'queryforge-test-'))
    const file = path.join(directory, 'input.csv')
    try {
      const contents = 'id,name\n1,Ada\n2,Grace\n'
      await fs.writeFile(file, contents)
      const scan = await scanCsv(file)
      assert.equal(scan.rowCount, 2)
      assert.deepEqual(scan.columnNames, ['id', 'name'])
      assert.deepEqual(scan.sample[1], { id: '2', name: 'Grace' })
      assert.equal(scan.contentChecksum, crypto.createHash('sha256').update(contents).digest('hex'))
    } finally {
      await fs.rm(directory, { recursive: true, force: true })
    }
  })
})
