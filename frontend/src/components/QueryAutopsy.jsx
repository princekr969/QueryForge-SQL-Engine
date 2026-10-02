import { useEffect, useState } from 'react'
import axios from 'axios'

const API_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:3000'
const compact = value => Number(value || 0).toLocaleString('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const operatorLessons = {
  PartitionScan: 'Reads one immutable partition, applying projection and predicates before transfer.',
  partition_scan: 'Reads one immutable partition, applying projection and predicates before transfer.',
  broadcast: 'Replicates the smaller join input to workers so the larger input does not shuffle.',
  hash_shuffle: 'Hash-partitions both join inputs so matching keys meet on the same worker.',
  local: 'Executes a cost-selected join locally when distribution would cost more than it saves.'
}

export default function QueryAutopsy ({ jobId, complete }) {
  const [report, setReport] = useState(null)
  const [open, setOpen] = useState(true)
  const [comparison, setComparison] = useState(null)
  const [comparing, setComparing] = useState(false)

  useEffect(() => {
    if (!jobId || !complete) return
    axios.get(`${API_URL}/api/query/jobs/${jobId}/autopsy`).then(response => setReport(response.data)).catch(() => {})
  }, [jobId, complete])

  if (!report) return null
  const maximum = Math.max(1, ...report.timeline.map(item => item.durationMs || 0))
  async function runWhatIf () {
    setComparing(true)
    try {
      const response = await axios.post(`${API_URL}/api/query/jobs/${jobId}/what-if`, {
        execute: true,
        workers: Math.min(8, Math.max(2, new Set(report.timeline.map(item => item.workerId)).size * 2)),
        combiner: true,
        cacheWarm: true
      })
      setComparison(response.data)
    } finally { setComparing(false) }
  }
  return (
    <section className="card-glow overflow-hidden" aria-labelledby="autopsy-title">
      <button onClick={() => setOpen(value => !value)} className="w-full px-5 py-4 flex items-center justify-between text-left bg-surface/40 border-b border-border">
        <div>
          <div className="text-[9px] font-mono tracking-[0.2em] uppercase text-warn-text">Post-execution evidence</div>
          <h2 id="autopsy-title" className="text-sm font-semibold text-ink mt-1">Query autopsy · {report.strategy}</h2>
        </div>
        <div className="flex items-center gap-2">
          <span className="badge-green">checksum {report.checksum?.slice(0, 8)}</span>
          <span className="text-ink-faint">{open ? '−' : '+'}</span>
        </div>
      </button>
      {open && <div className="p-5 space-y-5">
        <div className="grid grid-cols-2 sm:grid-cols-7 gap-4">
          {[
            ['scan', `${compact(report.totals.bytesScanned)}B`],
            ['wire', `${compact(report.totals.bytesTransferred)}B`],
            ['rows', compact(report.totals.rowsScanned)],
            ['peak rss', `${compact(report.totals.peakTaskMemoryBytes)}B`],
            ['cpu', `${compact(report.totals.cpuTimeMicros / 1000)}ms`],
            ['attempts', report.totals.attempts],
            ['straggler', `${report.stragglerRatio.toFixed(2)}×`]
          ].map(([label, value]) => <div key={label} className="border-l border-border pl-3"><div className="text-[9px] uppercase tracking-widest text-ink-ghost">{label}</div><div className="font-mono text-sm text-ink mt-1">{value}</div></div>)}
        </div>
        <div className="grid sm:grid-cols-[1fr_1fr_auto] gap-3 items-stretch">
          <div className="bg-void/60 border border-border rounded-lg p-3">
            <div className="text-[9px] uppercase tracking-widest text-ink-ghost">Dominant bottleneck</div>
            <div className="font-mono text-sm text-warn-text mt-1">{report.bottleneck.replaceAll('_', ' ')}</div>
          </div>
          <div className="bg-void/60 border border-border rounded-lg p-3">
            <div className="text-[9px] uppercase tracking-widest text-ink-ghost">Measured cost domains</div>
            <div className="flex flex-wrap gap-2 mt-1">{(report.costDomains || report.topCosts).map(cost => <span key={cost.name} className="badge-ghost" title={cost.evidence}>{cost.name} · {compact(cost.value)} {cost.unit}</span>)}</div>
            {report.costDomainNote && <div className="text-[9px] text-ink-ghost mt-2">{report.costDomainNote}</div>}
          </div>
          <button className="btn-primary" disabled={comparing} onClick={runWhatIf}>{comparing ? 'Running alternate…' : 'Run measured what-if'}</button>
        </div>
        {comparison?.measured && <div className="border border-accent/30 bg-accent/5 rounded-lg p-4" aria-live="polite">
          <div className="flex items-center justify-between"><div className="section-label !mb-0">Measured alternate plan</div><span className={comparison.measured.checksumMatch ? 'badge-green' : 'badge-red'}>{comparison.measured.checksumMatch ? 'checksum preserved' : 'checksum changed'}</span></div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-3 font-mono text-xs">
            <div><span className="text-ink-ghost">latency</span><div>{comparison.measured.baseline.latencyMs} → {comparison.measured.comparison.latencyMs} ms</div></div>
            <div><span className="text-ink-ghost">wire</span><div>{compact(comparison.measured.baseline.bytes)} → {compact(comparison.measured.comparison.bytes)} B</div></div>
            <div><span className="text-ink-ghost">cpu</span><div>{compact(comparison.measured.baseline.cpuMicros)} → {compact(comparison.measured.comparison.cpuMicros)} µs</div></div>
            <div><span className="text-ink-ghost">peak rss</span><div>{compact(comparison.measured.baseline.peakMemoryBytes)} → {compact(comparison.measured.comparison.peakMemoryBytes)} B</div></div>
          </div>
        </div>}
        <div>
          <div className="section-label">Operator evidence</div>
          <div className="grid sm:grid-cols-3 gap-2">{(report.topOperators || report.operators.slice(0, 3)).map((operator, index) => <details key={operator.taskId || `${operator.operator}-${index}`} className="border border-border rounded-lg p-3 group"><summary className="cursor-pointer list-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent rounded"><span className="text-accent font-mono">#{index + 1}</span> <span className="text-xs">{operator.operator}</span><div className="text-[10px] text-ink-faint mt-2">{operator.durationMs} ms · {compact(operator.scannedRows ?? operator.rows)} scanned · {compact(operator.producedRows)} produced</div>{operator.workerId && <div className="text-[9px] text-ink-ghost mt-1 font-mono">{operator.workerId} · attempt {operator.attempt}</div>}</summary><div className="text-[10px] text-ink-muted mt-3 border-t border-border pt-2"><p>{operatorLessons[operator.operator] || 'A measured physical stage executed by one or more workers.'}</p><dl className="grid grid-cols-2 gap-x-3 gap-y-1 mt-2 font-mono"><dt>CPU</dt><dd>{compact(operator.cpuTimeMicros)} µs</dd><dt>peak RSS</dt><dd>{compact(operator.peakMemoryBytes)} B</dd><dt>wire</dt><dd>{compact(operator.wireBytes)} B</dd><dt>spill</dt><dd>{compact(operator.spilledBytes)} B</dd><dt>throughput</dt><dd>{compact(operator.rowsPerSecond)} r/s</dd></dl></div></details>)}</div>
        </div>
        <div>
          <div className="section-label">Attempt timeline</div>
          <div className="space-y-2">
            {report.timeline.map(item => <div key={item.taskId} className="grid grid-cols-[76px_1fr_105px] gap-3 items-center text-[10px] font-mono">
              <span className="text-ink-faint truncate">{item.workerId}</span>
              <div className="h-4 bg-void border border-border rounded-sm overflow-hidden"><div className={`h-full ${item.status === 'failed' ? 'bg-danger/60' : item.winner ? 'bg-emerald-400/70' : 'bg-accent/60'}`} style={{ width: `${Math.max(2, ((item.durationMs || 0) / maximum) * 100)}%` }} /></div>
              <span className="text-right text-ink-muted">{item.durationMs ?? '—'}ms · {compact(item.rowsPerSecond)} r/s</span>
            </div>)}
          </div>
        </div>
        <div className="bg-void/60 border border-border rounded-lg p-4">
          <div className="text-[9px] uppercase tracking-widest text-ink-ghost mb-2">Optimizer notes</div>
          <ul className="space-y-1.5 text-xs text-ink-muted">{report.suggestions.map(item => <li key={item} className="flex gap-2"><span className="text-accent">→</span><span>{item}</span></li>)}</ul>
        </div>
      </div>}
    </section>
  )
}
