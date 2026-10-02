'use strict'

const crypto = require('crypto')

function hash64 (value, seed = 0) {
  const digest = crypto.createHash('sha256').update(`${seed}\0${String(value)}`).digest()
  return digest.readBigUInt64BE(0)
}

class HyperLogLog {
  constructor (precision = 12, registers) {
    if (!Number.isInteger(precision) || precision < 4 || precision > 16) throw new Error('HLL precision must be 4..16')
    this.precision = precision
    this.registers = registers ? Uint8Array.from(registers) : new Uint8Array(1 << precision)
  }

  add (value) {
    if (value === null || value === undefined || value === '') return
    const hash = hash64(value)
    const index = Number(hash >> BigInt(64 - this.precision))
    const remainder = (hash << BigInt(this.precision)) & ((1n << 64n) - 1n)
    const rank = Math.min(64 - this.precision + 1, remainder === 0n ? 64 - this.precision + 1 : Math.clz32(Number(remainder >> 32n)) + 1)
    this.registers[index] = Math.max(this.registers[index], rank)
  }

  merge (other) {
    if (other.precision !== this.precision) throw new Error('Cannot merge HLL sketches with different precision')
    for (let index = 0; index < this.registers.length; index++) {
      this.registers[index] = Math.max(this.registers[index], other.registers[index])
    }
    return this
  }

  estimate () {
    const m = this.registers.length
    const alpha = m === 16 ? 0.673 : m === 32 ? 0.697 : m === 64 ? 0.709 : 0.7213 / (1 + 1.079 / m)
    let inverse = 0
    let zeros = 0
    for (const register of this.registers) {
      inverse += 2 ** -register
      if (register === 0) zeros++
    }
    let estimate = alpha * m * m / inverse
    if (estimate <= 2.5 * m && zeros > 0) estimate = m * Math.log(m / zeros)
    return Math.max(0, Math.round(estimate))
  }

  toJSON () { return { type: 'hll', precision: this.precision, registers: Buffer.from(this.registers).toString('base64') } }
  static fromJSON (state) { return new HyperLogLog(state.precision, Buffer.from(state.registers, 'base64')) }
}

class KllSketch {
  constructor (capacity = 200, levels = [[]], compactions = []) {
    if (!Number.isInteger(capacity) || capacity < 20 || capacity > 5000) throw new Error('KLL capacity must be 20..5000')
    this.capacity = capacity
    this.levels = levels.map(level => level.map(Number))
    this.compactions = [...compactions]
  }

  add (value) {
    const number = Number(value)
    if (!Number.isFinite(number)) return
    this.levels[0].push(number)
    this.compact(0)
  }

  compact (level) {
    while ((this.levels[level]?.length || 0) > this.capacity) {
      const values = this.levels[level].sort((a, b) => a - b)
      const parity = (this.compactions[level] || 0) % 2
      this.compactions[level] = (this.compactions[level] || 0) + 1
      if (!this.levels[level + 1]) this.levels[level + 1] = []
      for (let index = parity; index < values.length; index += 2) this.levels[level + 1].push(values[index])
      this.levels[level] = values.length % 2 === 1 ? [values[parity === 0 ? values.length - 1 : 0]] : []
      level++
    }
  }

  merge (other) {
    if (other.capacity !== this.capacity) throw new Error('Cannot merge KLL sketches with different capacity')
    for (let level = 0; level < other.levels.length; level++) {
      if (!this.levels[level]) this.levels[level] = []
      this.levels[level].push(...other.levels[level])
      this.compact(level)
    }
    return this
  }

  quantile (q) {
    if (!Number.isFinite(q) || q < 0 || q > 1) throw new Error('Quantile must be 0..1')
    const weighted = []
    let total = 0
    this.levels.forEach((level, index) => {
      const weight = 2 ** index
      for (const value of level) { weighted.push({ value, weight }); total += weight }
    })
    if (total === 0) return null
    weighted.sort((a, b) => a.value - b.value)
    const target = q * (total - 1)
    let seen = 0
    for (const item of weighted) {
      seen += item.weight
      if (seen > target) return item.value
    }
    return weighted.at(-1).value
  }

  toJSON () { return { type: 'kll', capacity: this.capacity, levels: this.levels, compactions: this.compactions } }
  static fromJSON (state) { return new KllSketch(state.capacity, state.levels, state.compactions) }
}

class FrequencySketch {
  constructor (width = 2048, depth = 5, candidates = 20, tables, observed) {
    this.width = width
    this.depth = depth
    this.candidateLimit = candidates
    this.tables = tables ? tables.map(row => Uint32Array.from(row)) : Array.from({ length: depth }, () => new Uint32Array(width))
    this.observed = new Map(observed || [])
  }

  add (value, count = 1) {
    if (value === null || value === undefined) return
    const key = String(value)
    for (let depth = 0; depth < this.depth; depth++) {
      const index = Number(hash64(key, depth) % BigInt(this.width))
      this.tables[depth][index] += count
    }
    this.observed.set(key, this.estimate(key))
    if (this.observed.size > this.candidateLimit * 4) {
      const keep = [...this.observed].sort((a, b) => b[1] - a[1]).slice(0, this.candidateLimit * 2)
      this.observed = new Map(keep)
    }
  }

  estimate (value) {
    const key = String(value)
    let minimum = Infinity
    for (let depth = 0; depth < this.depth; depth++) {
      const index = Number(hash64(key, depth) % BigInt(this.width))
      minimum = Math.min(minimum, this.tables[depth][index])
    }
    return minimum === Infinity ? 0 : minimum
  }

  merge (other) {
    if (other.width !== this.width || other.depth !== this.depth) throw new Error('Frequency sketch dimensions differ')
    for (let depth = 0; depth < this.depth; depth++) {
      for (let index = 0; index < this.width; index++) this.tables[depth][index] += other.tables[depth][index]
    }
    for (const key of other.observed.keys()) this.observed.set(key, this.estimate(key))
    return this
  }

  topK (count = this.candidateLimit) {
    return [...this.observed.keys()].map(value => ({ value, estimate: this.estimate(value) }))
      .sort((a, b) => b.estimate - a.estimate || a.value.localeCompare(b.value)).slice(0, count)
  }

  toJSON () { return { type: 'frequency', width: this.width, depth: this.depth, candidates: this.candidateLimit, tables: this.tables.map(row => [...row]), observed: [...this.observed] } }
  static fromJSON (state) { return new FrequencySketch(state.width, state.depth, state.candidates, state.tables, state.observed) }
}

class PriorityReservoir {
  constructor (size = 100, entries = []) { this.size = size; this.entries = [...entries] }
  add (value, identity = value) {
    const priority = hash64(identity).toString(16).padStart(16, '0')
    this.entries.push({ priority, value })
    this.entries.sort((a, b) => a.priority.localeCompare(b.priority))
    if (this.entries.length > this.size) this.entries.length = this.size
  }
  merge (other) {
    this.entries.push(...other.entries)
    this.entries.sort((a, b) => a.priority.localeCompare(b.priority))
    this.entries.length = Math.min(this.entries.length, this.size)
    return this
  }
  values () { return this.entries.map(entry => entry.value) }
  toJSON () { return { type: 'reservoir', size: this.size, entries: this.entries } }
  static fromJSON (state) { return new PriorityReservoir(state.size, state.entries) }
}

class BloomFilter {
  constructor (bits = 262144, hashes = 5, bytes) {
    if (!Number.isInteger(bits) || bits < 1024 || bits > 16777216 || bits % 8 !== 0) throw new Error('Bloom bits must be a byte-aligned integer from 1024 to 16777216')
    if (!Number.isInteger(hashes) || hashes < 1 || hashes > 12) throw new Error('Bloom hashes must be 1..12')
    this.bits = bits
    this.hashes = hashes
    this.bytes = bytes ? Uint8Array.from(bytes) : new Uint8Array(bits / 8)
  }
  add (value) {
    if (value === null || value === undefined) return
    for (let seed = 0; seed < this.hashes; seed++) {
      const bit = Number(hash64(value, seed) % BigInt(this.bits))
      this.bytes[Math.floor(bit / 8)] |= 1 << (bit % 8)
    }
  }
  has (value) {
    if (value === null || value === undefined) return false
    for (let seed = 0; seed < this.hashes; seed++) {
      const bit = Number(hash64(value, seed) % BigInt(this.bits))
      if ((this.bytes[Math.floor(bit / 8)] & (1 << (bit % 8))) === 0) return false
    }
    return true
  }
  merge (other) {
    if (other.bits !== this.bits || other.hashes !== this.hashes) throw new Error('Bloom filter dimensions differ')
    for (let index = 0; index < this.bytes.length; index++) this.bytes[index] |= other.bytes[index]
    return this
  }
  toJSON () { return { type: 'bloom', bits: this.bits, hashes: this.hashes, bytes: Buffer.from(this.bytes).toString('base64') } }
  static fromJSON (state) { return new BloomFilter(state.bits, state.hashes, Buffer.from(state.bytes, 'base64')) }
}

function sketchFromJSON (state) {
  if (state.type === 'hll') return HyperLogLog.fromJSON(state)
  if (state.type === 'kll') return KllSketch.fromJSON(state)
  if (state.type === 'frequency') return FrequencySketch.fromJSON(state)
  if (state.type === 'reservoir') return PriorityReservoir.fromJSON(state)
  if (state.type === 'bloom') return BloomFilter.fromJSON(state)
  throw new Error(`Unknown sketch type: ${state.type}`)
}

module.exports = { HyperLogLog, KllSketch, FrequencySketch, PriorityReservoir, BloomFilter, sketchFromJSON }
