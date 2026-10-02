import { useEffect, useMemo, useRef, useState } from 'react'
import axios from 'axios'

const API_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:3000'

const concepts = [
  { code: '01', title: 'MapReduce', subtitle: 'Shrink before you shuffle', body: 'Mappers scan immutable partitions. A merge-safe combiner collapses repeated keys locally; the coordinator reduces partial states. Toggle the combiner to see communication cost become visible.', tags: ['map', 'combiner', 'shuffle', 'backup task'] },
  { code: '02', title: 'Spark-style lineage', subtitle: 'Remember how to rebuild', body: 'Transformations remain lazy until an action. Dataset, operator, and partition lineage records let QueryForge replay one missing partition from a durable ancestor.', tags: ['lazy DAG', 'cache', 'broadcast', 'accumulator'] },
  { code: '03', title: 'Data Streams', subtitle: 'Bound the infinite', body: 'Kafka offsets and window state commit together. Watermarks close event-time windows; late records enter an audit trail instead of silently changing finalized answers.', tags: ['TUMBLE', 'HOP', 'SESSION', 'watermark'] }
]

const questions = [
  { prompt: 'A mapper reads 1,000,000 rows but finds 20 local keys. What most directly cuts mapper→reducer traffic?', choices: ['A merge-safe combiner', 'A larger LIMIT', 'A second reducer'], answer: 0, note: 'The combiner sends roughly one state per local key instead of one state per row.' },
  { prompt: 'Which Spark-style operation should trigger execution?', choices: ['filter transformation', 'groupBy transformation', 'collect action'], answer: 2, note: 'Transformations extend lineage; actions demand a result.' },
  { prompt: 'What does an event-time watermark represent?', choices: ['A CPU deadline', 'Progress beyond which older events are late', 'A Kafka partition count'], answer: 1, note: 'Allowed lateness subtracts from the maximum observed event time.' }
]

export default function LearningLab ({ datasets }) {
  const [datasetId, setDatasetId] = useState('')
  const [lineage, setLineage] = useState({ nodes: [], edges: [] })
  const [answers, setAnswers] = useState({})
  const [stream, setStream] = useState(null)
  const [streamError, setStreamError] = useState('')
  const [loadingStream, setLoadingStream] = useState(false)
  const [lineageZoom, setLineageZoom] = useState(1)
  const [evidence, setEvidence] = useState(null)
  const lineageCanvas = useRef(null)
  const dragState = useRef(null)

  useEffect(() => { if (!datasetId && datasets[0]) setDatasetId(datasets[0].id) }, [datasets, datasetId])
  useEffect(() => {
    if (!datasetId) return
    axios.get(`${API_URL}/api/lineage`, { params: { datasetId } }).then(response => setLineage(response.data)).catch(() => {})
  }, [datasetId])
  useEffect(() => {
    axios.get(`${API_URL}/api/evidence/milestone2`).then(response => setEvidence(response.data)).catch(() => {})
  }, [])

  const layers = useMemo(() => {
    const order = ['source', 'transformation', 'partition', 'materialization']
    return order.map(kind => ({ kind, nodes: lineage.nodes.filter(node => node.kind === kind) })).filter(layer => layer.nodes.length)
  }, [lineage])

  function beginLineagePan (event) {
    const canvas = lineageCanvas.current
    if (!canvas) return
    canvas.setPointerCapture(event.pointerId)
    dragState.current = { x: event.clientX, y: event.clientY, left: canvas.scrollLeft, top: canvas.scrollTop }
    canvas.classList.add('cursor-grabbing')
  }

  function moveLineagePan (event) {
    const canvas = lineageCanvas.current
    if (!canvas || !dragState.current) return
    canvas.scrollLeft = dragState.current.left - (event.clientX - dragState.current.x)
    canvas.scrollTop = dragState.current.top - (event.clientY - dragState.current.y)
  }

  function endLineagePan () {
    lineageCanvas.current?.classList.remove('cursor-grabbing')
    dragState.current = null
  }

  async function startStreamDemo () {
    setLoadingStream(true)
    setStreamError('')
    try {
      const suffix = Date.now().toString().slice(-7)
      const registered = await axios.post(`${API_URL}/api/streams/register`, {
        name: `stage_demo_${suffix}`, topic: `stage_demo_${suffix}`, eventTimeColumn: 'ts',
        windowType: 'TUMBLE', sizeMs: 1000, allowedLatenessMs: 1500,
        distinctColumn: 'user_id', sumColumn: 'revenue', heavyHitterColumn: 'url', hllPrecision: 14,
        trackExactEvaluation: true
      })
      const base = Date.now()
      const events = Array.from({ length: 120 }, (_, index) => ({
        ts: new Date(base + Math.floor(index / 30) * 1000).toISOString(),
        user_id: `user-${index}`, revenue: 1 + (index % 17), url: ['/home', '/search', '/checkout'][index % 3]
      }))
      let snapshot = null
      for (let chunk = 0; chunk < 4; chunk++) {
        const published = (chunk + 1) * 30
        await axios.post(`${API_URL}/api/streams/publish/${registered.data.source_topic}`, { events: events.slice(chunk * 30, published) })
        for (let attempt = 0; attempt < 20; attempt++) {
          await new Promise(resolve => setTimeout(resolve, 100))
          snapshot = (await axios.get(`${API_URL}/api/streams/${registered.data.id}`)).data
          if (snapshot.batches.reduce((sum, batch) => sum + Number(batch.event_count), 0) >= published) break
        }
        setStream(snapshot)
        if (chunk < 3) await new Promise(resolve => setTimeout(resolve, 1000))
      }
    } catch (error) { setStreamError(error.response?.data?.error || error.message) } finally { setLoadingStream(false) }
  }

  return <div className="space-y-5 animate-fade-in">
    <section className="card-glow p-5 overflow-hidden">
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4">
        <div><div className="text-[9px] font-mono tracking-[0.24em] uppercase text-accent">CS404 systems map</div><h1 className="text-xl font-semibold mt-1">One engine, three execution models</h1><p className="text-xs text-ink-muted mt-1 max-w-2xl">Follow a bounded batch from MapReduce, through replayable lineage, into an unbounded event-time stream.</p></div>
        <div className="badge-green">presentation mode · live evidence</div>
      </div>
      <div className="grid md:grid-cols-3 gap-3 mt-5">
        {concepts.map((concept, index) => <article key={concept.title} className="relative border border-border bg-void/50 rounded-xl p-4 group hover:border-accent/40 transition-colors" tabIndex="0">
          <div className="text-[10px] font-mono text-ink-ghost">{concept.code} / lecture system</div><h2 className="font-semibold mt-2">{concept.title}</h2><div className="text-xs text-accent mt-0.5">{concept.subtitle}</div><p className="text-xs text-ink-muted mt-3 leading-relaxed">{concept.body}</p><div className="flex flex-wrap gap-1.5 mt-4">{concept.tags.map(tag => <span className="badge-ghost" key={tag}>{tag}</span>)}</div>{index < 2 && <span aria-hidden="true" className="hidden md:block absolute -right-5 top-1/2 z-10 text-accent bg-card border border-border rounded-full w-7 h-7 text-center leading-6">→</span>}
        </article>)}
      </div>
    </section>

    <section className="card-glow p-5" aria-labelledby="lineage-heading">
      <div className="flex flex-col sm:flex-row justify-between gap-3"><div><div className="section-label">Replayable evidence</div><h2 id="lineage-heading" className="font-semibold">Partition lineage graph</h2><p className="text-xs text-ink-muted">Partition/operator provenance is recovery-grade; row/cell ancestry is intentionally sampled, not claimed as complete.</p></div><div className="flex gap-2 items-center"><div className="flex rounded-lg border border-border overflow-hidden" aria-label="Lineage zoom controls"><button className="px-2.5 py-1 text-xs hover:bg-surface" onClick={() => setLineageZoom(value => Math.max(0.6, value - 0.2))} aria-label="Zoom lineage out">−</button><output className="px-2 py-1 text-[10px] font-mono border-x border-border min-w-12 text-center">{Math.round(lineageZoom * 100)}%</output><button className="px-2.5 py-1 text-xs hover:bg-surface" onClick={() => setLineageZoom(value => Math.min(1.8, value + 0.2))} aria-label="Zoom lineage in">+</button></div><select className="input-select sm:w-64" value={datasetId} onChange={event => setDatasetId(event.target.value)}>{datasets.map(dataset => <option key={dataset.id} value={dataset.id}>{dataset.name}</option>)}</select></div></div>
      <div ref={lineageCanvas} className="mt-5 overflow-auto pb-2 cursor-grab select-none touch-none" tabIndex="0" aria-label="Pan and zoom partition lineage canvas" onPointerDown={beginLineagePan} onPointerMove={moveLineagePan} onPointerUp={endLineagePan} onPointerCancel={endLineagePan}><div className="flex items-stretch gap-4 min-w-max transition-transform" style={{ transform: `scale(${lineageZoom})`, transformOrigin: 'left top', marginBottom: `${Math.max(0, (lineageZoom - 1) * 180)}px` }}>{layers.map((layer, index) => <div key={layer.kind} className="flex items-center gap-4"><div className="w-56 border border-border rounded-lg bg-surface/70 p-3"><div className="text-[9px] tracking-widest uppercase text-ink-ghost">{layer.kind}</div><div className="space-y-2 mt-2">{layer.nodes.slice(0, 5).map(node => <div key={node.id} className="border-l-2 border-accent/50 pl-2"><div className="font-mono text-[10px] text-ink truncate">{node.operator}</div><div className="text-[9px] text-ink-ghost truncate">{node.logical_partition_key || node.id.slice(0, 8)} · {node.status}</div></div>)}</div>{layer.nodes.length > 5 && <div className="text-[9px] text-ink-faint mt-2">+{layer.nodes.length - 5} more nodes</div>}</div>{index < layers.length - 1 && <span className="text-accent">→</span>}</div>)}</div></div>
      <div className="text-[10px] text-ink-ghost mt-2">{lineage.nodes.length} nodes · {lineage.edges.length} dependency edges</div>
    </section>

    <section className="card-glow p-5" aria-labelledby="ablation-heading">
      <div className="flex items-end justify-between gap-3"><div><div className="section-label">Artifact-backed ablations</div><h2 id="ablation-heading" className="font-semibold">Measured baseline vs optimized</h2><p className="text-xs text-ink-muted mt-1">Values are loaded from the versioned Milestone 2 benchmark artifact—not synthesized in the browser.</p></div>{evidence && <span className="badge-green">{evidence.verificationState.replaceAll('-', ' ')}</span>}</div>
      {evidence ? <div className="grid lg:grid-cols-3 gap-3 mt-4">{evidence.ablations.map(ablation => <article key={ablation.id} className="border border-border rounded-xl p-4 bg-void/40"><div className="text-xs font-semibold">{ablation.title}</div><div className="grid grid-cols-2 gap-2 mt-3"><div className="rounded-lg bg-danger/5 border border-danger/20 p-2"><div className="text-[9px] uppercase tracking-wider text-ink-ghost">{ablation.baseline.label}</div><div className="text-lg font-mono mt-1">{ablation.baseline.value}</div><div className="text-[9px] text-ink-faint">{ablation.baseline.unit}</div></div><div className="rounded-lg bg-success/5 border border-success/20 p-2"><div className="text-[9px] uppercase tracking-wider text-ink-ghost">{ablation.optimized.label}</div><div className="text-lg font-mono text-success-text mt-1">{ablation.optimized.value}</div><div className="text-[9px] text-ink-faint">{ablation.optimized.unit}</div></div></div><div className="text-[10px] text-accent mt-3">{ablation.improvement}</div><div className="text-[9px] text-ink-ghost mt-1">✓ {ablation.invariant}</div></article>)}</div> : <div className="text-xs text-ink-muted mt-4">Evidence artifact is not available in this deployment.</div>}
    </section>

    <section className="grid lg:grid-cols-2 gap-5">
      <div className="card-glow p-5"><div className="section-label">Live Kafka window</div><h2 className="font-semibold">120-event standing-query trial</h2><p className="text-xs text-ink-muted mt-1">Publishes four one-second micro-batches into TUMBLE windows with exact revenue, HLL users, count-min top URLs, and a 1.5-second watermark.</p><button onClick={startStreamDemo} disabled={loadingStream} className="btn-primary mt-4">{loadingStream ? 'Processing live micro-batches…' : 'Run live stream demo'}</button>{streamError && <p className="text-xs text-danger-text mt-3">{streamError}</p>}{stream && <div className="mt-4 space-y-2" aria-live="polite"><div className="flex flex-wrap gap-2"><span className="badge-green">{stream.batches.reduce((sum, batch) => sum + Number(batch.event_count), 0)} committed events</span><span className="badge-blue">HLL bound {(stream.windows[0]?.result.distinctConfiguredError * 100 || 0).toFixed(3)}%</span><span className="badge-blue">observed {(stream.windows[0]?.result.distinctObservedError * 100 || 0).toFixed(3)}%</span><span className="badge-ghost">CMS bound {(stream.windows[0]?.result.frequencyConfiguredError * 100 || 0).toFixed(3)}%</span><span className="badge-ghost">{stream.acceptedLateEvents} accepted late · {stream.lateEvents} audited</span></div>{stream.windows.slice(-3).map(window => <div key={window.window_key} className="grid grid-cols-[1fr_auto_auto] gap-3 border border-border rounded-lg p-2 text-[10px] font-mono"><span>{new Date(window.window_start).toLocaleTimeString()}</span><span>{Math.round(window.result.distinctUsers)} users</span><span>₹{window.result.revenue}</span></div>)}</div>}</div>
      <div className="card-glow p-5"><div className="section-label">Professor mode</div><h2 className="font-semibold">Three-question systems check</h2><div className="space-y-4 mt-4">{questions.map((question, qIndex) => <fieldset key={question.prompt}><legend className="text-xs text-ink mb-2">{qIndex + 1}. {question.prompt}</legend><div className="flex flex-wrap gap-2">{question.choices.map((choice, cIndex) => <button key={choice} onClick={() => setAnswers(value => ({ ...value, [qIndex]: cIndex }))} className={`px-2.5 py-1.5 rounded-md border text-[10px] transition-colors ${answers[qIndex] === cIndex ? (cIndex === question.answer ? 'border-success/50 bg-success/10 text-success-text' : 'border-danger/50 bg-danger/10 text-danger-text') : 'border-border text-ink-muted hover:border-accent/40'}`}>{choice}</button>)}</div>{answers[qIndex] !== undefined && <p className="text-[10px] text-ink-faint mt-1.5">{answers[qIndex] === question.answer ? 'Correct — ' : 'Try again — '}{question.note}</p>}</fieldset>)}</div></div>
    </section>
  </div>
}
