'use strict'

function inferType (sample, columnName) {
  for (const row of sample.slice(0, 100)) {
    const value = row[columnName]
    if (value === null || value === undefined || value === '') continue
    if (!Number.isFinite(Number(value))) return 'string'
  }
  return 'number'
}

function partitionIndexForRow (rowIndex, rowCount, partitionCount) {
  if (!Number.isInteger(rowIndex) || rowIndex < 0) throw new Error('rowIndex must be non-negative')
  if (!Number.isInteger(rowCount) || rowCount < 1) throw new Error('rowCount must be positive')
  if (!Number.isInteger(partitionCount) || partitionCount < 1) throw new Error('partitionCount must be positive')
  const chunkSize = Math.ceil(rowCount / partitionCount)
  return Math.min(Math.floor(rowIndex / chunkSize), partitionCount - 1)
}

module.exports = { inferType, partitionIndexForRow }
