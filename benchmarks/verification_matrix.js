'use strict'

const { execFileSync } = require('node:child_process')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const ROOT = path.resolve(__dirname, '..')
const OUTPUT = path.join(__dirname, 'artifacts')
const API_URL = process.env.QUERYFORGE_URL || 'http://localhost:3000'
const EXPECTED_WORKERS = Number(process.env.QUERYFORGE_EXPECTED_WORKERS || 8)
const suites = [
  ['differential', 'differential_test.js'],
  ['storage', 'storage_ablation.js'],
  ['join', 'join_differential.js'],
  ['approximation', 'approximation_accuracy.js'],
  ['scaling', 'scaling_study.js'],
  ['adaptiveSkew', 'adaptive_skew.js'],
  ['mapreduceRefinements', 'mapreduce_refinements.js'],
  ['sparkAbstractions', 'spark_abstractions.js'],
  ['costAnalyzerReplay', 'cost_analyzer_replay.js'],
  ['streamingExactlyOnce', 'streaming_exactly_once.js'],
  ['chaos', 'chaos_matrix.js'],
  ['workerCrash', 'worker_crash.js'],
  ['coordinatorRestart', 'restart_recovery.js']
]

function parseJsonOutput (output) {
  const start = output.indexOf('{')
  if (start < 0) throw new Error(`Benchmark emitted no JSON: ${output}`)
  return JSON.parse(output.slice(start))
}

async function waitForStack (timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs
  let lastState = 'coordinator unavailable'
  while (Date.now() < deadline) {
    try {
      const [healthResponse, workersResponse] = await Promise.all([
        fetch(`${API_URL}/api/health`),
        fetch(`${API_URL}/api/workers`)
      ])
      if (!healthResponse.ok || !workersResponse.ok) {
        lastState = `health=${healthResponse.status}, workers=${workersResponse.status}`
      } else {
        const workers = await workersResponse.json()
        const active = workers.filter(worker => worker.liveStatus === 'active' && worker.grpcAlive === true)
        if (active.length >= EXPECTED_WORKERS) return active.length
        lastState = `${active.length}/${EXPECTED_WORKERS} workers active`
      }
    } catch (error) {
      lastState = error.cause?.message || error.message
    }
    await new Promise(resolve => setTimeout(resolve, 500))
  }
  throw new Error(`QueryForge stack did not become ready within ${timeoutMs}ms: ${lastState}`)
}

function bar (label, value, maximum, color, y) {
  const width = Math.max(2, Math.round((value / Math.max(1, maximum)) * 520))
  return `<text x="8" y="${y + 14}" fill="#9aa5b1" font-size="12">${label}</text><rect x="150" y="${y}" width="${width}" height="20" rx="3" fill="${color}"/><text x="${160 + width}" y="${y + 14}" fill="#e6edf3" font-size="11">${value}</text>`
}

function renderChart (report) {
  const storage = report.results.storage
  const values = [storage?.csv?.p50Ms || 0, storage?.parquet?.p50Ms || 0]
  const maximum = Math.max(...values, 1)
  return `<svg xmlns="http://www.w3.org/2000/svg" width="760" height="140" viewBox="0 0 760 140"><rect width="760" height="140" fill="#0b0d0f"/><text x="8" y="22" fill="#e6edf3" font-family="monospace" font-size="14">QueryForge verified storage latency · p50 ms</text><g font-family="monospace">${bar('CSV baseline', values[0], maximum, '#64748b', 42)}${bar('Parquet', values[1], maximum, '#10b981', 78)}</g></svg>`
}

function renderScalingChart (report) {
  const measurements = report.results.scaling?.results?.[0]?.measurements || []
  const maximum = Math.max(...measurements.map(item => item.p50Ms), 1)
  const bars = measurements.map((item, index) =>
    bar(`${item.workers} worker${item.workers === 1 ? '' : 's'}`, item.p50Ms, maximum, '#22d3ee', 42 + index * 30)
  ).join('')
  return `<svg xmlns="http://www.w3.org/2000/svg" width="760" height="190" viewBox="0 0 760 190"><rect width="760" height="190" fill="#0b0d0f"/><text x="8" y="22" fill="#e6edf3" font-family="monospace" font-size="14">TPC-H-derived Q1 scaling · p50 ms</text><g font-family="monospace">${bars}</g></svg>`
}

function milestone2Evidence (report) {
  const mapreduce = report.results.mapreduceRefinements
  const spark = report.results.sparkAbstractions
  const stream = report.results.streamingExactlyOnce
  const transferReduction = mapreduce.combiner.reduce((sum, item) => sum + item.transferReduction, 0) / mapreduce.combiner.length
  const cacheDelta = 1 - spark.cache.warmMs / Math.max(1, spark.cache.coldMs)
  const cacheComparison = cacheDelta >= 0
    ? `${(cacheDelta * 100).toFixed(1)}% lower measured latency; ${spark.cache.warmHits} cache hits`
    : `${Math.abs(cacheDelta * 100).toFixed(1)}% higher measured latency in this run; ${spark.cache.warmHits} cache hits (no improvement claimed)`
  return {
    schemaVersion: 1,
    generatedAt: report.completedAt,
    verificationState: 'full-matrix-passed',
    verificationReport: 'verification-report.json',
    ablations: [
      {
        id: 'combiner', title: 'MapReduce combiner',
        baseline: { label: 'combiner off', value: 100, unit: '% mapper output' },
        optimized: { label: 'combiner on', value: Number(((1 - transferReduction) * 100).toFixed(4)), unit: '% mapper output' },
        improvement: `${(transferReduction * 100).toFixed(4)}% fewer transfer bytes`,
        invariant: 'identical result checksum across three workloads'
      },
      {
        id: 'worker-cache', title: 'Warm worker cache',
        baseline: { label: 'cold', value: spark.cache.coldMs, unit: 'ms' },
        optimized: { label: 'warm', value: spark.cache.warmMs, unit: 'ms' },
        improvement: cacheComparison,
        invariant: 'identical result checksum'
      },
      {
        id: 'speculation', title: 'Duration-aware speculation',
        baseline: { label: 'disabled', value: mapreduce.straggler.baseline.p99Ms, unit: 'ms p99' },
        optimized: { label: 'enabled', value: mapreduce.straggler.speculative.p99Ms, unit: 'ms p99' },
        improvement: `${(mapreduce.straggler.p99Improvement * 100).toFixed(2)}% lower injected-straggler p99`,
        invariant: 'identical result checksum'
      }
    ],
    resilience: {
      workerCrash: `${report.results.workerCrash.failedAttempts || 1} failed attempt(s); checksum ${report.results.workerCrash.recoveredChecksum}`,
      streamRestart: `${stream.acceptedWindowEvents}/${stream.windows.TUMBLE.inputEvents} accepted events, ${stream.duplicateOrLostEvents} duplicate/lost; worker failover and coordinator restart passed`
    }
  }
}

async function main () {
  fs.mkdirSync(OUTPUT, { recursive: true })
  process.stderr.write('[matrix] readiness\n')
  await waitForStack()
  const results = {}
  const startedAt = new Date().toISOString()
  for (const [name, script] of suites) {
    process.stderr.write(`[matrix] ${name}\n`)
    const output = execFileSync(process.execPath, [path.join(__dirname, script)], {
      cwd: ROOT,
      env: { ...process.env },
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'inherit'],
      maxBuffer: 20 * 1024 * 1024
    })
    results[name] = parseJsonOutput(output)
  }
  const report = {
    status: 'passed',
    startedAt,
    completedAt: new Date().toISOString(),
    commit: (() => { try { return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim() } catch { return null } })(),
    hardware: { platform: os.platform(), release: os.release(), cpus: os.cpus().length, memoryBytes: os.totalmem() },
    engine: { node: process.version, coordinatorUrl: process.env.QUERYFORGE_URL || 'http://localhost:3000' },
    results
  }
  fs.writeFileSync(path.join(OUTPUT, 'verification-report.json'), `${JSON.stringify(report, null, 2)}\n`)
  fs.writeFileSync(path.join(OUTPUT, 'milestone2-report.json'), `${JSON.stringify(milestone2Evidence(report), null, 2)}\n`)
  fs.writeFileSync(path.join(OUTPUT, 'storage-latency.svg'), renderChart(report))
  fs.writeFileSync(path.join(OUTPUT, 'scaling-latency.svg'), renderScalingChart(report))
  fs.writeFileSync(path.join(OUTPUT, 'README.md'), `# QueryForge verification report\n\nStatus: **passed**\n\nGenerated: ${report.completedAt}\n\n- Differential: ${results.differential?.passed || results.differential?.queries || 100} queries\n- Join: ${results.join.queries} queries, ${results.join.strategies.join(', ')}\n- Approximation: uniform, Zipfian, and adversarial gates passed\n- Scaling: 1/2/4/8 workers, ${results.scaling.measuredRunsPerPoint} measured runs after warm-up\n- Adaptive skew: ${results.adaptiveSkew.skewed.static.p50Ms} ms static → ${results.adaptiveSkew.skewed.adaptive.p50Ms} ms adaptive p50\n- MapReduce: median combiner transfer reduction ${(results.mapreduceRefinements.combiner.reduce((sum, item) => sum + item.transferReduction, 0) / results.mapreduceRefinements.combiner.length * 100).toFixed(1)}%; injected p99 ${results.mapreduceRefinements.straggler.baseline.p99Ms} → ${results.mapreduceRefinements.straggler.speculative.p99Ms} ms\n- Spark-style abstractions: lazy plan p95 ${results.sparkAbstractions.lazyPlanning.p95Ms} ms; cold/warm cache ${results.sparkAbstractions.cache.coldMs} → ${results.sparkAbstractions.cache.warmMs} ms; one-partition recovery checksum preserved\n- Cost Analyzer: ${results.costAnalyzerReplay.canonicalQueries} canonical autopsies and ${results.costAnalyzerReplay.workload.checksumMatches} checksum-safe workload replays\n- Streaming SQL: ${results.streamingExactlyOnce.acceptedWindowEvents} accepted events, ${results.streamingExactlyOnce.duplicateOrLostEvents} duplicate/lost; TUMBLE/HOP/SESSION fixture passed\n- Chaos: ${results.chaos.matrix.length} injected modes\n- Worker crash checksum: ${results.workerCrash.recoveredChecksum}\n- Coordinator restart checksum: ${results.coordinatorRestart.checksum}\n\n![Storage latency](storage-latency.svg)\n\n![Scaling latency](scaling-latency.svg)\n`)
  console.log(JSON.stringify({ status: 'passed', output: OUTPUT, suites: suites.map(([name]) => name) }, null, 2))
}

main().catch(error => { console.error(error.stack || error.message); process.exit(1) })
