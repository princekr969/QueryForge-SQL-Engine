'use strict'

function percentile (values, quantile) {
  if (values.length === 0) return 0
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * quantile))]
}

function speculationThresholdMs (completedDurations, minimumMs = 250) {
  return Math.max(minimumMs, Math.ceil(percentile(completedDurations, 0.75) * 1.5))
}

module.exports = { percentile, speculationThresholdMs }
