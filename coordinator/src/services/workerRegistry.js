'use strict'

// Side-effect-free source of truth for live worker membership. Keeping the map
// outside the gRPC server lets planners and tests inspect it without starting
// heartbeat timers merely by importing a service module.
const workerRegistry = new Map()

module.exports = { workerRegistry }
