import { useMemo, useState } from 'react'
import axios from 'axios'

const API_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:3000'

const OPERATIONS = [
  { value: 'approx_count_distinct', label: 'Distinct count', detail: 'HyperLogLog' },
  { value: 'approx_percentile', label: 'Percentile', detail: 'KLL sketch' },
  { value: 'heavy_hitters', label: 'Heavy hitters', detail: 'Count-min' },
  { value: 'sample', label: 'Preview sample', detail: 'Priority reservoir' },
]

const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 5 })

function Metric ({ label, value, accent = false }) {
  return (
    <div className="border-l border-border pl-3">
      <div className="text-[9px] uppercase tracking-[0.16em] text-ink-ghost">{label}</div>
      <div className={`mt-1 font-mono text-sm ${accent ? 'text-emerald-300' : 'text-ink'}`}>{value}</div>
    </div>
  )
}

export default function ApproximateWorkbench ({ datasets }) {
  const [datasetId, setDatasetId] = useState('')
  const [column, setColumn] = useState('')
  const [operation, setOperation] = useState('approx_count_distinct')
  const [exactValue, setExactValue] = useState('')
  const [parameter, setParameter] = useState('')
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)

  const dataset = datasets.find(item => item.id === datasetId)
  const columns = useMemo(() => dataset?.schema_json?.columns || [], [dataset])

  function chooseDataset (value) {
    setDatasetId(value)
    const selected = datasets.find(item => item.id === value)
    setColumn(selected?.schema_json?.columns?.[0]?.name || '')
    setResult(null)
  }

  async function run (event) {
    event.preventDefault()
    setLoading(true)
    setError(null)
    setResult(null)
    const body = { datasetId, column, operation }
    if (exactValue !== '') body.exactValue = Number(exactValue)
    if (operation === 'approx_count_distinct') body.precision = Number(parameter || 12)
    if (operation === 'approx_percentile') { body.quantile = Number(parameter || 0.95); body.capacity = 400 }
    if (operation === 'heavy_hitters') body.k = Number(parameter || 10)
    if (operation === 'sample') body.sampleSize = Number(parameter || 64)
    try {
      const response = await axios.post(`${API_URL}/api/approximate`, body)
      setResult(response.data)
    } catch (requestError) {
      setError(requestError.response?.data?.error || requestError.message)
    } finally {
      setLoading(false)
    }
  }

  const estimate = result?.result?.estimate
  const comparisonRatio = result?.operation === 'approx_count_distinct' && result?.observedError != null && result?.configuredError
    ? Math.min(100, (result.observedError / result.configuredError) * 100)
    : null

  return (
    <section className="grid grid-cols-1 lg:grid-cols-5 gap-5 animate-fade-in" aria-labelledby="approx-title">
      <form onSubmit={run} className="card-glow lg:col-span-2 overflow-hidden">
        <div className="px-5 py-4 border-b border-border bg-surface/40">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[9px] font-mono tracking-[0.2em] text-accent uppercase">Sketch laboratory</p>
              <h2 id="approx-title" className="text-lg font-semibold text-ink mt-1">Approximate analytics</h2>
            </div>
            <span className="badge-green">Distributed</span>
          </div>
          <p className="text-xs text-ink-faint mt-2">Each worker returns one mergeable state. Raw values stay at the partition.</p>
        </div>

        <div className="p-5 space-y-4">
          <label className="block text-xs text-ink-muted">
            <span className="block mb-1.5">Dataset</span>
            <select className="input-select" value={datasetId} onChange={event => chooseDataset(event.target.value)} required>
              <option value="">Select a dataset</option>
              {datasets.map(item => <option key={item.id} value={item.id}>{item.name} · {item.row_count?.toLocaleString()} rows</option>)}
            </select>
          </label>

          <div className="grid grid-cols-2 gap-3">
            <label className="block text-xs text-ink-muted">
              <span className="block mb-1.5">Column</span>
              <select className="input-select" value={column} onChange={event => setColumn(event.target.value)} required>
                <option value="">Select</option>
                {columns.map(item => <option key={item.name} value={item.name}>{item.name} ({item.type})</option>)}
              </select>
            </label>
            <label className="block text-xs text-ink-muted">
              <span className="block mb-1.5">Reference value</span>
              <input className="input-base" type="number" step="any" value={exactValue} onChange={event => setExactValue(event.target.value)} placeholder="optional exact" />
            </label>
          </div>

          <fieldset>
            <legend className="text-xs text-ink-muted mb-2">Algorithm</legend>
            <div className="grid grid-cols-2 gap-2">
              {OPERATIONS.map(item => (
                <button key={item.value} type="button" aria-pressed={operation === item.value}
                  onClick={() => { setOperation(item.value); setParameter(''); setResult(null) }}
                  className={`text-left p-3 rounded-lg border transition-all ${operation === item.value ? 'border-accent/50 bg-accent/10' : 'border-border bg-surface hover:border-navy'}`}>
                  <span className="block text-xs font-medium text-ink">{item.label}</span>
                  <span className="block text-[10px] font-mono text-ink-ghost mt-0.5">{item.detail}</span>
                </button>
              ))}
            </div>
          </fieldset>

          <label className="block text-xs text-ink-muted">
            <span className="block mb-1.5">
              {operation === 'approx_count_distinct' ? 'Precision (4–16)' : operation === 'approx_percentile' ? 'Quantile (0–1)' : operation === 'heavy_hitters' ? 'Top K' : 'Sample size'}
            </span>
            <input className="input-base font-mono" type="number" step={operation === 'approx_percentile' ? '0.01' : '1'} value={parameter}
              onChange={event => setParameter(event.target.value)} placeholder={operation === 'approx_count_distinct' ? '12' : operation === 'approx_percentile' ? '0.95' : operation === 'heavy_hitters' ? '10' : '64'} />
          </label>

          <button className="btn-primary w-full min-h-11" disabled={!datasetId || !column || loading}>
            {loading ? 'Merging partition sketches…' : 'Run bounded estimate'}
          </button>
          {error && <div role="alert" className="text-xs text-danger-text bg-danger-dim border border-danger/20 rounded-lg p-3">{error}</div>}
        </div>
      </form>

      <div className="card-glow lg:col-span-3 min-h-[520px] relative overflow-hidden">
        <div aria-hidden className="absolute inset-0 opacity-30" style={{ backgroundImage: 'repeating-linear-gradient(135deg,transparent,transparent 18px,rgba(59,130,246,.055) 18px,rgba(59,130,246,.055) 19px)' }} />
        {!result ? (
          <div className="relative h-full min-h-[520px] flex flex-col items-center justify-center text-center px-8">
            <div className="font-mono text-6xl text-border">±</div>
            <h3 className="text-base font-semibold text-ink mt-4">Error is part of the result</h3>
            <p className="text-xs text-ink-faint mt-2 max-w-sm">Supply an exact reference to plot observed error against the configured statistical envelope.</p>
          </div>
        ) : (
          <div className="relative p-6 space-y-6" aria-live="polite">
            <div className="flex items-start justify-between border-b border-border pb-5">
              <div>
                <p className="text-[9px] font-mono tracking-[0.2em] text-emerald-300 uppercase">Run {result.runId.slice(0, 8)}</p>
                <div className="text-4xl sm:text-5xl font-mono font-medium text-ink mt-2 tracking-tight">
                  {estimate != null ? number.format(estimate) : `${result.result.items?.length || 0} items`}
                </div>
                <p className="text-xs text-ink-faint mt-2">{result.operation} · {result.partitions} partition states merged</p>
              </div>
              <span className="badge-blue">{result.executionTimeMs} ms</span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <Metric label="Rows scanned" value={number.format(result.rowsScanned)} />
              <Metric label="Wire bytes" value={number.format(result.transferredBytes)} accent />
              <Metric label="Final state" value={`${number.format(result.stateBytes)} B`} />
              <Metric label="Bound" value={result.configuredError == null ? 'deterministic' : number.format(result.configuredError)} />
            </div>

            {comparisonRatio != null && (
              <div className="bg-surface/70 border border-border rounded-xl p-4">
                <div className="flex items-center justify-between text-xs mb-3">
                  <span className="text-ink-muted">Observed / configured error</span>
                  <span className="font-mono text-emerald-300">{number.format(result.observedError)} / {number.format(result.configuredError)}</span>
                </div>
                <div className="h-2 bg-void rounded-full overflow-hidden border border-border">
                  <div className={`h-full transition-all duration-500 ${comparisonRatio <= 100 ? 'bg-emerald-400' : 'bg-danger'}`} style={{ width: `${comparisonRatio}%` }} />
                </div>
                <p className="text-[10px] text-ink-ghost mt-2">{comparisonRatio <= 100 ? 'Observed error is inside the configured envelope.' : 'Observed error exceeded the configured envelope.'}</p>
              </div>
            )}

            {result.result.items && (
              <div>
                <div className="section-label">Merged output</div>
                <div className="max-h-56 overflow-auto border border-border rounded-lg bg-void/70">
                  {result.result.items.map((item, index) => (
                    <div key={`${index}-${JSON.stringify(item)}`} className="flex items-center justify-between px-3 py-2 border-b border-border/50 last:border-0 text-xs font-mono">
                      <span className="text-ink-muted truncate">{typeof item === 'object' ? item.value : String(item)}</span>
                      {typeof item === 'object' && <span className="text-emerald-300 ml-3">{number.format(item.estimate)}</span>}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  )
}
