'use strict'

const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const ROWS = Number(process.env.ROWS || 60000)

async function request (path, options) {
  const response = await fetch(`${API_URL}${path}`, options)
  const payload = await response.json()
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(payload)}`)
  return payload
}

function buildDataset () {
  const lines = ['uniform_id,zipf_key,value,adversarial_key']
  const values = []
  for (let index = 0; index < ROWS; index++) {
    const bucket = index % 100
    const zipf = bucket < 50 ? 'hot' : bucket < 75 ? 'warm' : bucket < 88 ? 'cool' : `tail_${index % 97}`
    const value = ((index * 7919) % 100003) - 50000
    const adversarial = `key_${index % 17003}_${index % 2 ? 'Aa' : 'BB'}`
    values.push(value)
    lines.push(`${index},${zipf},${value},${adversarial}`)
  }
  return { csv: `${lines.join('\n')}\n`, values }
}

async function upload (csv) {
  const body = new FormData()
  body.append('file', new Blob([csv], { type: 'text/csv' }), 'approximation_accuracy.csv')
  return request('/api/datasets/upload', { method: 'POST', body })
}

async function approximate (body) {
  return request('/api/approximate', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body)
  })
}

function rankError (sorted, estimate, quantile) {
  let low = 0
  while (low < sorted.length && sorted[low] < estimate) low++
  let high = low
  while (high < sorted.length && sorted[high] <= estimate) high++
  const observedRank = ((low + high - 1) / 2) / Math.max(1, sorted.length - 1)
  return Math.abs(observedRank - quantile)
}

async function main () {
  const generated = buildDataset()
  const uploaded = await upload(generated.csv)
  const base = { datasetId: uploaded.datasetId, maxExecutionMs: 60000 }

  const uniform = await approximate({
    ...base, operation: 'approx_count_distinct', column: 'uniform_id', precision: 12, exactValue: ROWS
  })
  if (uniform.observedError > uniform.configuredError * 2) {
    throw new Error(`Uniform HLL error ${uniform.observedError} exceeded tolerance`)
  }

  const adversarialDistinct = new Set(Array.from({ length: ROWS }, (_, index) => `key_${index % 17003}_${index % 2 ? 'Aa' : 'BB'}`)).size
  const adversarial = await approximate({
    ...base, operation: 'approx_count_distinct', column: 'adversarial_key', precision: 12,
    exactValue: adversarialDistinct
  })
  if (adversarial.observedError > adversarial.configuredError * 2) {
    throw new Error(`Adversarial HLL error ${adversarial.observedError} exceeded tolerance`)
  }

  const sorted = [...generated.values].sort((a, b) => a - b)
  const quantile = 0.95
  const exactP95 = sorted[Math.floor(quantile * (sorted.length - 1))]
  const percentile = await approximate({
    ...base, operation: 'approx_percentile', column: 'value', quantile, capacity: 400, exactValue: exactP95
  })
  const percentileRankError = rankError(sorted, percentile.result.estimate, quantile)
  if (percentileRankError > percentile.configuredError) {
    throw new Error(`KLL rank error ${percentileRankError} exceeded ${percentile.configuredError}`)
  }

  const heavy = await approximate({
    ...base, operation: 'heavy_hitters', column: 'zipf_key', width: 2048, depth: 5, k: 3
  })
  const heavyValues = heavy.result.items.map(item => item.value)
  if (heavyValues[0] !== 'hot' || heavyValues[1] !== 'warm' || heavyValues[2] !== 'cool') {
    throw new Error(`Heavy hitters were wrong: ${heavyValues.join(', ')}`)
  }

  const sampleRequest = { ...base, operation: 'sample', column: 'adversarial_key', sampleSize: 64 }
  const sampleOne = await approximate(sampleRequest)
  const sampleTwo = await approximate(sampleRequest)
  if (JSON.stringify(sampleOne.result.items) !== JSON.stringify(sampleTwo.result.items)) {
    throw new Error('Priority reservoir was not deterministic across distributed runs')
  }
  if (sampleOne.result.items.length !== 64) throw new Error('Priority reservoir returned the wrong sample size')

  const rawPayloadEstimate = Buffer.byteLength(generated.csv)
  if (uniform.transferredBytes >= rawPayloadEstimate) throw new Error('HLL transferred more bytes than the raw dataset')

  console.log(JSON.stringify({
    status: 'passed',
    datasetId: uploaded.datasetId,
    rows: ROWS,
    distributions: {
      uniform: {
        operation: uniform.operation,
        exact: ROWS,
        estimate: uniform.result.estimate,
        observedRelativeError: uniform.observedError,
        configuredRelativeError: uniform.configuredError,
        transferredBytes: uniform.transferredBytes,
        rawPayloadEstimate
      },
      adversarial: {
        exact: adversarialDistinct,
        estimate: adversarial.result.estimate,
        observedRelativeError: adversarial.observedError,
        configuredRelativeError: adversarial.configuredError
      },
      percentile: {
        quantile,
        exact: exactP95,
        estimate: percentile.result.estimate,
        observedRankError: percentileRankError,
        configuredRankError: percentile.configuredError
      },
      zipfian: { expected: ['hot', 'warm', 'cool'], observed: heavy.result.items }
    },
    deterministicSample: { size: sampleOne.result.items.length, repeatable: true },
    runs: [uniform, adversarial, percentile, heavy, sampleOne, sampleTwo].map(run => ({
      runId: run.runId,
      operation: run.operation,
      stateBytes: run.stateBytes,
      transferredBytes: run.transferredBytes,
      executionTimeMs: run.executionTimeMs
    }))
  }, null, 2))
}

main().catch(error => {
  console.error(error.stack || error.message)
  process.exit(1)
})
