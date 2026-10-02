'use strict'

// Side-effect-free source of truth for live worker membership. Keeping the map
// outside the gRPC server lets planners and tests inspect it without starting
// heartbeat timers merely by importing a service module.
const workerRegistry = new Map()

function compareWorkerSchedulingOrder (left, right) {
  const leftFixed = /^worker-(\d+)$/.exec(left.workerId)
  const rightFixed = /^worker-(\d+)$/.exec(right.workerId)
  if (leftFixed && rightFixed) return Number(leftFixed[1]) - Number(rightFixed[1])
  if (leftFixed) return -1
  if (rightFixed) return 1
  return left.workerId < right.workerId ? -1 : left.workerId > right.workerId ? 1 : 0
}

module.exports = { workerRegistry, compareWorkerSchedulingOrder }
