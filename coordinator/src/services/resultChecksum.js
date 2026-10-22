'use strict'

const crypto = require('crypto')
const { mergeResults } = require('./resultMerger')

function stableJson (value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().filter(key => value[key] !== undefined)
      .map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

function semanticRowsChecksum (rows) {
  const canonicalRows = rows.map(stableJson).sort()
  return crypto.createHash('sha256').update(JSON.stringify(canonicalRows)).digest('hex')
}

function partialResultsChecksum (partialResults, plan) {
  return semanticRowsChecksum(mergeResults(partialResults, plan))
}

module.exports = { partialResultsChecksum, semanticRowsChecksum, stableJson }
