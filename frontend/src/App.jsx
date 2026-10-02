import { useState, useEffect, useCallback } from 'react'
import { Link } from 'react-router-dom'
import axios from 'axios'

import DatasetUploader from './components/DatasetUploader'
import SQLEditor       from './components/SQLEditor'
import WorkerDashboard from './components/WorkerDashboard'
import ApproximateWorkbench from './components/ApproximateWorkbench'
import QueryAutopsy from './components/QueryAutopsy'
import LearningLab from './components/LearningLab'
import Walkthrough from './components/Walkthrough'
import ResultsTable    from './components/ResultsTable'
import { useQuery }    from './hooks/useQuery'
import { useWebSocket } from './hooks/useWebSocket'

const API_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:3000'

export default function App () {
  const [datasets,        setDatasets]        = useState([])
  const [rows,            setRows]            = useState([])
  const [columns,         setColumns]         = useState([])
  const [progress,        setProgress]        = useState(null)
  const [queryComplete,   setQueryComplete]   = useState(false)
  const [executionTimeMs, setExecutionTimeMs] = useState(null)
  const [activeTab,       setActiveTab]       = useState('query') // 'query' | 'approximate' | 'datasets'

  const { jobId, loading, error, submitQuery } = useQuery()

  useEffect(() => {
    axios.get(`${API_URL}/api/datasets`)
      .then(res => setDatasets(res.data))
      .catch(() => {})
  }, [])

  const handleWsMessage = useCallback((msg) => {
    if (msg.type === 'row') {
      setRows(prev => [...prev, msg.data])
      setColumns(prev => prev.length === 0 ? Object.keys(msg.data) : prev)
    } else if (msg.type === 'progress') {
      setProgress({ completedTasks: msg.completedTasks, totalTasks: msg.totalTasks })
    } else if (msg.type === 'complete') {
      setQueryComplete(true)
      setExecutionTimeMs(msg.executionTimeMs)
    } else if (msg.type === 'error') {
      setQueryComplete(true)
    }
  }, [])

  useWebSocket(jobId, handleWsMessage)

  async function handleRunQuery (sql, datasetId) {
    setRows([])
    setColumns([])
    setProgress(null)
    setQueryComplete(false)
    setExecutionTimeMs(null)
    await submitQuery(sql, datasetId)
  }

  function handleDatasetUploaded () {
    axios.get(`${API_URL}/api/datasets`)
      .then(res => setDatasets(res.data))
      .catch(() => {})
  }

  const hasResults = rows.length > 0 || queryComplete

  return (
    <div className="min-h-screen bg-void text-ink flex flex-col">

      {/* ── Top grid glow ──────────────────────────────────────── */}
      <div
        className="fixed inset-0 pointer-events-none"
        aria-hidden="true"
        style={{
          backgroundImage: `
            linear-gradient(rgba(41,48,73,0.14) 1px, transparent 1px),
            linear-gradient(90deg, rgba(41,48,73,0.14) 1px, transparent 1px)
          `,
          backgroundSize: '32px 32px',
          maskImage: 'radial-gradient(ellipse 80% 60% at 50% 0%, black 20%, transparent 80%)',
          WebkitMaskImage: 'radial-gradient(ellipse 80% 60% at 50% 0%, black 20%, transparent 80%)',
        }}
      />
      {/* Accent glow at top */}
      <div
        className="fixed top-0 left-0 right-0 h-[500px] pointer-events-none"
        aria-hidden="true"
        style={{ background: 'radial-gradient(ellipse 60% 35% at 50% -5%, rgba(59,130,246,0.1) 0%, transparent 70%)' }}
      />

      {/* ── Header ─────────────────────────────────────────────── */}
      <header className="relative z-20 flex items-center justify-between px-6 py-3.5 border-b border-border/60 backdrop-blur-sm bg-void/80">
        {/* Logo */}
        <div className="flex items-center gap-3">
          <Link to="/" className="flex items-center gap-3 group">
            <div className="relative flex-shrink-0">
              <div className="w-8 h-8 rounded-lg bg-accent/10 border border-accent/20 flex items-center justify-center group-hover:border-accent/40 transition-colors">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                  <path d="M2 4h12M2 8h8M2 12h9" stroke="#3b82f6" strokeWidth="1.5" strokeLinecap="round"/>
                  <circle cx="13" cy="12" r="2.5" fill="#3b82f6"/>
                  <path d="M12 12l.8.8 1.6-1.6" stroke="white" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              </div>
            </div>
            <div>
              <div className="text-sm font-bold tracking-tight text-ink leading-none">QueryForge</div>
              <div className="text-[10px] text-ink-faint leading-none mt-0.5">Distributed SQL Engine</div>
            </div>
          </Link>
        </div>

        {/* Nav tabs */}
        <nav className="flex items-center gap-1 bg-surface border border-border rounded-lg p-1">
          {[
            { id: 'query',    label: 'Query',    icon: '⌘' },
            { id: 'approximate', label: 'Sketch Lab', icon: '±' },
            { id: 'datasets', label: 'Datasets', icon: '⊞' },
            { id: 'learn', label: 'Systems Lab', icon: '⌁' },
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`
                flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-150
                ${activeTab === tab.id
                  ? 'bg-card text-ink border border-border shadow-card'
                  : 'text-ink-muted hover:text-ink'
                }
              `}
            >
              <span className="opacity-60">{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </nav>

        {/* Right — status */}
        <div className="flex items-center gap-3">
          <div className="hidden sm:flex items-center gap-1.5 text-xs text-ink-faint">
            <span className="status-dot bg-success animate-pulse-dot" />
            <span>Engine online</span>
          </div>
          <div className="badge-ghost text-[10px] hidden md:flex">
            Inspired by AWS Athena · BigQuery
          </div>
        </div>
      </header>

      {/* ── Main content ───────────────────────────────────────── */}
      <main className="relative z-10 flex-1 flex flex-col max-w-6xl mx-auto w-full px-4 sm:px-6 py-6 gap-5">

        {activeTab === 'datasets' ? (
          /* ── Datasets view ─────────────────────────────────── */
          <div className="animate-fade-in">
            <div className="section-label">Dataset Management</div>
            <DatasetUploader
              datasets={datasets}
              onDatasetUploaded={handleDatasetUploaded}
            />
          </div>
        ) : activeTab === 'approximate' ? (
          <ApproximateWorkbench datasets={datasets} />
        ) : activeTab === 'learn' ? (
          <LearningLab datasets={datasets} />
        ) : (
          /* ── Query view ─────────────────────────────────────── */
          <>
            {/* Top row: SQL editor + worker status side-by-side on lg */}
            <div className="grid grid-cols-1 lg:grid-cols-5 gap-5 animate-fade-in">
              {/* SQL Editor — wider */}
              <div className="lg:col-span-3">
                <SQLEditor
                  datasets={datasets}
                  onRunQuery={handleRunQuery}
                  loading={loading}
                />
              </div>
              {/* Worker Dashboard — narrower */}
              <div className="lg:col-span-2">
                <WorkerDashboard progress={progress} active={!!jobId} />
              </div>
            </div>

            {/* Error */}
            {error && (
              <div className="flex items-start gap-3 p-4 bg-danger-dim border border-danger/20 rounded-xl text-sm text-danger-text animate-fade-in">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="mt-0.5 flex-shrink-0">
                  <circle cx="8" cy="8" r="7" stroke="#ef4444" strokeWidth="1.5"/>
                  <path d="M8 5v4M8 11v.5" stroke="#ef4444" strokeWidth="1.5" strokeLinecap="round"/>
                </svg>
                <span>{error}</span>
              </div>
            )}

            {/* Results */}
            {hasResults && (
              <div className="animate-fade-in">
                <ResultsTable
                  rows={rows}
                  columns={columns}
                  complete={queryComplete}
                  executionTimeMs={executionTimeMs}
                  loading={loading && !queryComplete}
                />
              </div>
            )}

            <QueryAutopsy jobId={jobId} complete={queryComplete} />

            {/* Empty state — no query run yet */}
            {!hasResults && !loading && !error && (
              <div className="flex flex-col items-center justify-center py-16 text-center animate-fade-in">
                <div className="w-14 h-14 rounded-2xl bg-card border border-border flex items-center justify-center mb-4">
                  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                    <path d="M3 6h18M3 12h14M3 18h16" stroke="#4a5568" strokeWidth="1.5" strokeLinecap="round"/>
                    <circle cx="20" cy="18" r="3.5" fill="#293049" stroke="#3b82f6" strokeWidth="1"/>
                    <path d="M19 18l.8.8 1.6-1.6" stroke="#3b82f6" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </div>
                <p className="text-ink-muted text-sm font-medium">Write a query and hit Run</p>
                <p className="text-ink-faint text-xs mt-1">Results stream live as workers compute</p>
              </div>
            )}
          </>
        )}
      </main>
      <Walkthrough />

      {/* ── Footer ─────────────────────────────────────────────── */}
      <footer className="relative z-10 flex items-center justify-between px-6 py-3 border-t border-border/40 text-[10px] text-ink-ghost">
        <span>QueryForge © 2026 · Prince Kumar</span>
        <span className="hidden sm:block">MapReduce · Replayable lineage · Kafka windows · Measured evidence</span>
        <span>v2.0.0-course</span>
      </footer>
    </div>
  )
}
