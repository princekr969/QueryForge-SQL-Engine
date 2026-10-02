'use strict'

const assert = require('node:assert/strict')
const { describe, it } = require('node:test')
const { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas } = require('../coordinator/src/services/queryPlanner')

const schema = {
  columns: [
    { name: 'region', type: 'string' },
    { name: 'sales', type: 'number' }
  ]
}

describe('queryPlanner — accepted compatibility grammar', () => {
  it('plans grouped aggregates with independent aliases', () => {
    const plan = buildExecutionPlan(
      "SELECT region, SUM(sales) AS total, AVG(sales) AS average FROM orders WHERE sales >= 10 AND region = 'west' GROUP BY region ORDER BY total DESC LIMIT 5"
    )
    assert.equal(plan.tableName, 'orders')
    assert.deepEqual(plan.selectColumns, ['region'])
    assert.deepEqual(plan.groupByColumns, ['region'])
    assert.deepEqual(plan.aggregations, [
      { function: 'SUM', column: 'sales', alias: 'total' },
      { function: 'AVG', column: 'sales', alias: 'average' }
    ])
    assert.equal(plan.predicates.length, 2)
    assert.equal(plan.limit, 5)
  })

  it('plans a global aggregate', () => {
    const plan = buildExecutionPlan('SELECT COUNT(*) AS n FROM orders')
    assert.deepEqual(plan.groupByColumns, [])
    assert.deepEqual(plan.aggregations, [{ function: 'COUNT', column: '*', alias: 'n' }])
  })

  it('plans a qualified broadcast join with mergeable local aggregate state', () => {
    const plan = buildExecutionPlan(
      'SELECT c.region AS region, SUM(o.sales) AS total FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE o.sales > 10 GROUP BY c.region ORDER BY total DESC'
    )
    assert.equal(plan.tables.length, 2)
    assert.equal(plan.join.leftColumn, 'customer_id')
    assert.deepEqual(plan.groupByColumns, ['region'])
    assert.deepEqual(plan.aggregations, [{ function: 'SUM', column: 'sales', alias: 'total' }])
    assert.match(plan.workerSql, /INNER JOIN/)
    assert.match(plan.workerSql, /SUM\("o"\."sales"\) AS "__v0"/)
    assert.match(plan.workerSqlUncombined, /"o"\."sales" AS "__r0"/)
    assert.doesNotMatch(plan.workerSqlUncombined, /GROUP BY/)
  })

  it('binds both join inputs and rejects mismatched key types', () => {
    const plan = buildExecutionPlan(
      'SELECT o.sales AS sales, c.region AS region FROM orders o INNER JOIN customers c ON o.customer_id = c.id'
    )
    const orders = { name: 'orders', schema_json: { columns: [
      { name: 'sales', type: 'number' }, { name: 'customer_id', type: 'number' }
    ] } }
    const customers = { name: 'customers', schema_json: { columns: [
      { name: 'id', type: 'string' }, { name: 'region', type: 'string' }
    ] } }
    assert.throws(() => validateJoinPlanAgainstSchemas(plan, [orders, customers]), /Join key type mismatch/)
    customers.schema_json.columns[0].type = 'number'
    assert.equal(validateJoinPlanAgainstSchemas(plan, [orders, customers]), plan)
  })
})

describe('queryPlanner — catalog schema binding', () => {
  it('annotates numeric ordering', () => {
    const plan = buildExecutionPlan('SELECT sales FROM orders ORDER BY sales')
    validatePlanAgainstSchema(plan, schema)
    assert.equal(plan.orderByType, 'number')
  })

  it('rejects unknown columns before scheduling', () => {
    const plan = buildExecutionPlan('SELECT missing FROM orders')
    assert.throws(() => validatePlanAgainstSchema(plan, schema), /Unknown column: missing/)
  })

  it('rejects numeric aggregates over string columns', () => {
    const plan = buildExecutionPlan('SELECT SUM(region) FROM orders')
    assert.throws(() => validatePlanAgainstSchema(plan, schema), /SUM requires a numeric column/)
  })

  it('rejects ORDER BY columns absent from the result', () => {
    const plan = buildExecutionPlan('SELECT region FROM orders ORDER BY sales')
    assert.throws(() => validatePlanAgainstSchema(plan, schema), /ORDER BY column must appear/)
  })
})

describe('queryPlanner — fail closed', () => {
  const rejected = [
    ['OR', 'SELECT * FROM orders WHERE a = 1 OR b = 2'],
    ['multiple statements', 'SELECT * FROM orders; SELECT * FROM orders'],
    ['exactly one INNER JOIN', 'SELECT orders.id FROM orders LEFT JOIN users ON orders.user_id = users.id'],
    ['expressions', 'SELECT sales * 2 FROM orders'],
    ['SELECT DISTINCT', 'SELECT DISTINCT region FROM orders'],
    ['selected columns must appear in GROUP BY', 'SELECT region, SUM(sales) FROM orders'],
    ['GROUP BY without an aggregate', 'SELECT region FROM orders GROUP BY region']
  ]

  for (const [message, sql] of rejected) {
    it(`rejects ${message}`, () => {
      assert.throws(() => buildExecutionPlan(sql), new RegExp(message))
    })
  }
})
