import { useEffect, useRef, useState } from 'react'

const steps = [
  ['MapReduce workspace', 'Run SQL against immutable Parquet partitions. Watch mapper progress, combiner traffic, retries, and one committed winner.'],
  ['Evidence, not decoration', 'Query Autopsy is built from task counters: CPU, RSS, scan bytes, wire bytes, retries, critical path, and checksum.'],
  ['Spark ideas, native runtime', 'Lazy plans, lineage replay, cache levels, broadcast reuse, and accumulators run on QueryForge’s Arrow/gRPC engine.'],
  ['Unbounded mode', 'The Learning Lab starts Kafka standing queries with event-time windows, watermarks, sketches, and materialization.']
]

export default function Walkthrough () {
  const [step, setStep] = useState(-1)
  const dialog = useRef(null)
  useEffect(() => { if (!localStorage.getItem('queryforge-walkthrough-v2')) setStep(0) }, [])
  const close = () => { localStorage.setItem('queryforge-walkthrough-v2', 'seen'); setStep(-1) }
  useEffect(() => {
    if (step < 0) return
    dialog.current?.focus()
    const onKeyDown = event => {
      if (event.key === 'Escape') close()
      if (event.key === 'ArrowRight') setStep(value => value === steps.length - 1 ? value : value + 1)
      if (event.key === 'ArrowLeft') setStep(value => Math.max(0, value - 1))
      if (event.key === 'Tab') {
        const focusable = [...(dialog.current?.querySelectorAll('button') || [])]
        if (!focusable.length) return
        const first = focusable[0]
        const last = focusable.at(-1)
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus() }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus() }
      }
    }
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, [step])
  if (step < 0) return null
  return <div className="fixed inset-0 z-50 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4" role="dialog" aria-modal="true" aria-labelledby="walkthrough-title">
    <div ref={dialog} tabIndex="-1" className="w-full max-w-lg card-glow p-6 relative focus:outline-none"><div className="text-[10px] font-mono tracking-[0.2em] text-accent uppercase">60-second architecture tour · {step + 1}/{steps.length}</div><h2 id="walkthrough-title" className="text-xl font-semibold mt-2">{steps[step][0]}</h2><p className="text-sm text-ink-muted mt-3 leading-relaxed">{steps[step][1]}</p><div className="h-1 bg-surface rounded mt-6 overflow-hidden"><div className="h-full bg-accent transition-all" style={{ width: `${((step + 1) / steps.length) * 100}%` }} /></div><p className="text-[10px] text-ink-ghost mt-3">Use ←/→ to navigate, Esc to dismiss.</p><div className="flex justify-between mt-4"><button className="btn-ghost" onClick={close}>Skip tour</button><div className="flex gap-2">{step > 0 && <button className="btn-ghost" onClick={() => setStep(step - 1)}>Back</button>}<button className="btn-primary" onClick={() => step === steps.length - 1 ? close() : setStep(step + 1)}>{step === steps.length - 1 ? 'Enter QueryForge' : 'Next concept'}</button></div></div></div>
  </div>
}
