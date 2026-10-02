import { useEffect, useState } from 'react'
import axios from 'axios'

const API_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:3000'

const STATUS_CONFIG = {
  idle:       { dot: 'bg-ink-faint',           label: 'Idle',       ring: 'border-border' },
  processing: { dot: 'bg-warn animate-pulse',   label: 'Processing', ring: 'border-warn/30' },
  done:       { dot: 'bg-success',              label: 'Done',       ring: 'border-success/30' },
  dead:       { dot: 'bg-danger',               label: 'Offline',    ring: 'border-danger/20' },
  unknown:    { dot: 'bg-ink-ghost',            label: 'Unknown',    ring: 'border-border' },
}

export default function WorkerDashboard ({ progress, active }) {
  const [workers, setWorkers] = useState([])
  const [pulse,   setPulse]   = useState(false)

  useEffect(() => {
    async function fetchWorkers () {
      try {
        const res = await axios.get(`${API_URL}/api/workers`)
        setWorkers(res.data)
        setPulse(true)
        setTimeout(() => setPulse(false), 300)
      } catch { /* ignore */ }
    }
    fetchWorkers()
    const id = setInterval(fetchWorkers, 3000)
    return () => clearInterval(id)
  }, [])

  const completedTasks = progress?.completedTasks ?? 0
  const totalTasks     = progress?.totalTasks     ?? 0
  const pct            = totalTasks > 0 ? (completedTasks / totalTasks) * 100 : 0
  const allDone        = totalTasks > 0 && completedTasks === totalTasks

  const displayWorkers = workers.length > 0
    ? workers
    : ['worker-1', 'worker-2', 'worker-3'].map(id => ({ id, liveStatus: 'unknown', activeTasks: 0 }))

  return (
    <div className="card-glow h-full flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-2">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <rect x="1" y="1" width="5" height="5" rx="1.5" stroke="#8892a4" strokeWidth="1.2"/>
            <rect x="8" y="1" width="5" height="5" rx="1.5" stroke="#8892a4" strokeWidth="1.2"/>
            <rect x="1" y="8" width="5" height="5" rx="1.5" stroke="#8892a4" strokeWidth="1.2"/>
            <rect x="8" y="8" width="5" height="5" rx="1.5" stroke="#3b82f6" strokeWidth="1.2"/>
          </svg>
          <span className="text-xs font-semibold text-ink">Worker Cluster</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className={`status-dot ${active ? 'bg-warn animate-pulse' : 'bg-ink-faint'}`} />
          <span className="text-[10px] text-ink-faint">{active ? 'Active' : 'Standby'}</span>
        </div>
      </div>

      {/* Progress bar */}
      <div className="px-4 py-3 border-b border-border/50">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] text-ink-faint uppercase tracking-wider font-medium">Task Progress</span>
          <span className="text-[10px] font-mono text-ink-muted">
            {totalTasks > 0 ? `${completedTasks}/${totalTasks}` : '—'}
          </span>
        </div>
        <div className="h-1.5 bg-surface rounded-full overflow-hidden border border-border/50">
          <div
            className="h-full rounded-full transition-all duration-500 ease-out"
            style={{
              width: `${pct}%`,
              background: allDone
                ? 'linear-gradient(90deg, #10b981, #34d399)'
                : 'linear-gradient(90deg, #3b82f6, #60a5fa)',
              boxShadow: pct > 0 ? `0 0 8px ${allDone ? 'rgba(16,185,129,0.4)' : 'rgba(59,130,246,0.4)'}` : 'none',
            }}
          />
        </div>
        {totalTasks > 0 && (
          <div className="mt-1.5 text-[10px] text-ink-faint">
            {allDone
              ? <span className="text-success-text">All tasks complete</span>
              : `${Math.round(pct)}% complete`
            }
          </div>
        )}
      </div>

      {/* Worker cards */}
      <div className="flex-1 p-3 grid grid-cols-1 gap-2">
        {displayWorkers.map(w => (
          <WorkerCard key={w.id} worker={w} pulse={pulse && w.liveStatus === 'processing'} />
        ))}
      </div>

      {/* Footer */}
      <div className="px-4 py-2.5 border-t border-border/50 flex items-center justify-between">
        <span className="text-[10px] text-ink-ghost">{displayWorkers.length} nodes · gRPC</span>
        <span className="text-[10px] text-ink-ghost">heartbeat 5s</span>
      </div>
    </div>
  )
}

function WorkerCard ({ worker }) {
  const status = worker.activeTasks > 0
    ? 'processing'
    : worker.liveStatus === 'active' ? 'idle' : (worker.liveStatus || 'unknown')
  const cfg    = STATUS_CONFIG[status] || STATUS_CONFIG.unknown

  return (
    <div className={`
      relative flex items-center justify-between
      px-3 py-2.5 rounded-lg border bg-surface/60
      transition-all duration-300
      ${cfg.ring}
      ${status === 'processing' ? 'animate-glow-pulse' : ''}
    `}>
      {/* Left */}
      <div className="flex items-center gap-2.5 min-w-0">
        <div className={`
          w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0
          ${status === 'processing' ? 'bg-warn/10' : status === 'done' ? 'bg-success-dim' : status === 'dead' ? 'bg-danger-dim' : 'bg-card'}
        `}>
          {status === 'processing' ? (
            <svg className="animate-spin" width="10" height="10" viewBox="0 0 10 10" fill="none">
              <circle cx="5" cy="5" r="3.5" stroke="rgba(245,158,11,0.3)" strokeWidth="1.5"/>
              <path d="M5 1.5A3.5 3.5 0 0 1 8.5 5" stroke="#f59e0b" strokeWidth="1.5" strokeLinecap="round"/>
            </svg>
          ) : status === 'done' ? (
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
              <path d="M2 5l2.5 2.5L8 3" stroke="#10b981" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          ) : status === 'dead' ? (
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
              <path d="M3 3l4 4M7 3l-4 4" stroke="#ef4444" strokeWidth="1.5" strokeLinecap="round"/>
            </svg>
          ) : (
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
              <rect x="2" y="2" width="6" height="6" rx="1" stroke="#4a5568" strokeWidth="1.2"/>
            </svg>
          )}
        </div>
        <div className="min-w-0">
          <div className="text-xs font-mono font-medium text-ink leading-none">{worker.id}</div>
          {worker.activeTasks > 0 && (
            <div className="text-[9px] text-ink-faint mt-0.5">{worker.activeTasks} task{worker.activeTasks !== 1 ? 's' : ''}</div>
          )}
        </div>
      </div>

      {/* Right */}
      <div className="flex items-center gap-1.5">
        <span className={`status-dot ${cfg.dot}`} />
        <span className={`
          text-[10px] font-medium
          ${status === 'processing' ? 'text-warn-text'
            : status === 'done'   ? 'text-success-text'
            : status === 'dead'   ? 'text-danger-text'
            : 'text-ink-faint'}
        `}>{cfg.label}</span>
      </div>

      {/* Scan line for processing */}
      {status === 'processing' && (
        <div
          aria-hidden="true"
          className="absolute left-0 right-0 h-px bg-gradient-to-r from-transparent via-warn/40 to-transparent animate-scan pointer-events-none"
        />
      )}
    </div>
  )
}
