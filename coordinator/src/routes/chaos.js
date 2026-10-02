'use strict'

const express = require('express')
const chaos = require('../services/chaosController')

const router = express.Router()

router.get('/', (_req, res) => res.json({ rule: chaos.snapshot() }))

router.put('/', (req, res) => {
  try {
    res.json({ rule: chaos.configure(req.body) })
  } catch (error) {
    res.status(400).json({ error: error.message })
  }
})

router.delete('/', (_req, res) => res.json({ cleared: chaos.clear() }))

module.exports = router
