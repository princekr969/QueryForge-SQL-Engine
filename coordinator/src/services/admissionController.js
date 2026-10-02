'use strict'

class AdmissionController {
  constructor ({ maxConcurrent = 4, maxQueued = 100 } = {}) {
    this.maxConcurrent = maxConcurrent
    this.maxQueued = maxQueued
    this.active = 0
    this.sequence = 0
    this.queue = []
  }

  admit ({ jobId, priority = 0, run }) {
    if (this.queue.length >= this.maxQueued) {
      throw new Error(`Admission queue is full (${this.maxQueued})`)
    }
    let resolve
    let reject
    const promise = new Promise((ok, fail) => { resolve = ok; reject = fail })
    this.queue.push({ jobId, priority, run, resolve, reject, sequence: this.sequence++ })
    this.queue.sort((left, right) => right.priority - left.priority || left.sequence - right.sequence)
    const position = this.queue.findIndex(item => item.jobId === jobId) + 1
    this.drain()
    return { promise, position }
  }

  drain () {
    while (this.active < this.maxConcurrent && this.queue.length > 0) {
      const item = this.queue.shift()
      this.active++
      Promise.resolve()
        .then(item.run)
        .then(item.resolve, item.reject)
        .finally(() => {
          this.active--
          this.drain()
        })
    }
  }

  snapshot () {
    return {
      active: this.active,
      maxConcurrent: this.maxConcurrent,
      queued: this.queue.map(({ jobId, priority }, index) => ({ jobId, priority, position: index + 1 }))
    }
  }
}

const admissionController = new AdmissionController({
  maxConcurrent: Number(process.env.MAX_CONCURRENT_QUERIES || 4),
  maxQueued: Number(process.env.MAX_QUEUED_QUERIES || 100)
})

module.exports = { AdmissionController, admissionController }
