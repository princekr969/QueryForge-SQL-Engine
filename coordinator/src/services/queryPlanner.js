'use strict'

/**
 * queryPlanner.js
 * Parses SQL using node-sql-parser and generates an execution plan.
 */

const { Parser } = require('node-sql-parser')
const { trace }  = require('@opentelemetry/api')

const parser = new Parser()
const tracer = trace.getTracer('coordinator')
const SUPPORTED_COMPARISON_OPERATORS = new Set(['=', '!=', '<>', '>', '<', '>=', '<='])
const SUPPORTED_AGGREGATIONS = new Set(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])

function unsupported (detail) {
  throw new Error(`Unsupported SQL: ${detail}`)
}

function quoteIdentifier (value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

function renderQualifiedColumn (node, aliases) {
  if (!node || node.type !== 'column_ref' || !node.table || !aliases.has(node.table)) {
    unsupported('joined columns must be qualified with a table alias')
  }
  return `${quoteIdentifier(node.table)}.${quoteIdentifier(node.column)}`
}

function renderJoinPredicates (node, aliases, predicates = []) {
  if (!node) return ''
  if (node.type === 'binary_expr' && String(node.operator).toUpperCase() === 'AND') {
    const left = renderJoinPredicates(node.left, aliases, predicates)
    const right = renderJoinPredicates(node.right, aliases, predicates)
    return `${left} AND ${right}`
  }
  if (node.type !== 'binary_expr' || !SUPPORTED_COMPARISON_OPERATORS.has(node.operator)) {
    unsupported('joined WHERE supports only AND-joined comparisons')
  }
  const column = renderQualifiedColumn(node.left, aliases)
  const literal = node.right
  if (!['number', 'single_quote_string', 'string', 'double_quote_string'].includes(literal?.type)) {
    unsupported('joined WHERE predicates must compare a qualified column with a literal')
  }
  const type = literal.type === 'number' ? 'number' : 'string'
  const value = String(literal.value)
  predicates.push({ table: node.left.table, column: node.left.column, operator: node.operator, value, type })
  const renderedValue = type === 'number'
    ? String(Number(value))
    : `'${value.replace(/'/g, "''")}'`
  return `${column} ${node.operator} ${renderedValue}`
}

function buildJoinPlan (ast) {
  if (ast.from.length !== 2 || !ast.from[1].join || ast.from[1].join !== 'INNER JOIN') {
    unsupported('exactly one INNER JOIN is supported')
  }
  const [left, right] = ast.from
  if (!left.table || !right.table || left.expr || right.expr) unsupported('join subqueries')
  const leftAlias = left.as || left.table
  const rightAlias = right.as || right.table
  if (leftAlias === rightAlias) unsupported('join aliases must be unique')
  const aliases = new Set([leftAlias, rightAlias])
  const on = right.on
  if (on?.type !== 'binary_expr' || on.operator !== '=' ||
      on.left?.type !== 'column_ref' || on.right?.type !== 'column_ref') {
    unsupported('JOIN ON must be one equality between qualified columns')
  }
  const onLeft = renderQualifiedColumn(on.left, aliases)
  const onRight = renderQualifiedColumn(on.right, aliases)

  if (!Array.isArray(ast.columns) || ast.columns.length === 0) unsupported('joined SELECT *')
  const projection = []
  const aggregations = []
  for (const [index, item] of ast.columns.entries()) {
    if (item.expr?.type === 'column_ref') {
      const expression = renderQualifiedColumn(item.expr, aliases)
      projection.push({
        table: item.expr.table,
        column: item.expr.column,
        alias: item.as || item.expr.column,
        expression
      })
      continue
    }
    if (item.expr?.type !== 'aggr_func') unsupported('joined SELECT expressions must be qualified columns or aggregates')
    const fn = String(item.expr.name).toUpperCase()
    if (!SUPPORTED_AGGREGATIONS.has(fn) || item.expr.args?.distinct) unsupported(`joined aggregate ${fn}`)
    const argument = item.expr.args?.expr
    const star = argument?.type === 'star'
    if (!star && argument?.type !== 'column_ref') unsupported(`${fn} requires a qualified column or *`)
    if (star && fn !== 'COUNT') unsupported(`${fn}(*)`)
    const expression = star ? '*' : renderQualifiedColumn(argument, aliases)
    aggregations.push({
      function: fn,
      table: star ? '' : argument.table,
      column: star ? '*' : argument.column,
      alias: item.as || `${fn.toLowerCase()}_${star ? 'all' : argument.column}`,
      expression,
      index
    })
  }

  const groupNodes = Array.isArray(ast.groupby) ? ast.groupby : (ast.groupby?.columns || [])
  const groupBy = groupNodes.map(node => {
    const expression = renderQualifiedColumn(node, aliases)
    const projected = projection.find(item => item.table === node.table && item.column === node.column)
    if (!projected) unsupported('joined GROUP BY columns must appear in SELECT')
    return { ...projected, expression }
  })
  if (aggregations.length > 0) {
    const grouped = new Set(groupBy.map(item => `${item.table}.${item.column}`))
    const missing = projection.filter(item => !grouped.has(`${item.table}.${item.column}`))
    if (missing.length) unsupported('joined non-aggregate columns must appear in GROUP BY')
  } else if (groupBy.length) unsupported('GROUP BY without an aggregate')

  const predicates = []
  const renderedWhere = renderJoinPredicates(ast.where, aliases, predicates)
  const selectSql = groupBy.map(item => `${item.expression} AS ${quoteIdentifier(item.alias)}`)
  if (aggregations.length) selectSql.push('COUNT(*) AS "__rows"')
  aggregations.forEach((aggregation, index) => {
    if (aggregation.function === 'COUNT') {
      selectSql.push(`COUNT(${aggregation.expression}) AS "__c${index}"`)
    } else if (aggregation.function === 'AVG') {
      selectSql.push(`SUM(${aggregation.expression}) AS "__v${index}"`)
      selectSql.push(`COUNT(${aggregation.expression}) AS "__c${index}"`)
    } else {
      selectSql.push(`${aggregation.function}(${aggregation.expression}) AS "__v${index}"`)
      selectSql.push(`COUNT(${aggregation.expression}) AS "__c${index}"`)
    }
  })
  if (!aggregations.length) {
    selectSql.push(...projection.map(item => `${item.expression} AS ${quoteIdentifier(item.alias)}`))
  }
  const groupSql = groupBy.length ? ` GROUP BY ${groupBy.map(item => item.expression).join(', ')}` : ''
  const whereSql = renderedWhere ? ` WHERE ${renderedWhere}` : ''
  const workerSql = `SELECT ${selectSql.join(', ')} FROM ${quoteIdentifier(leftAlias)} INNER JOIN ${quoteIdentifier(rightAlias)} ON ${onLeft} = ${onRight}${whereSql}${groupSql}`
  const rawSelectSql = [
    ...groupBy.map(item => `${item.expression} AS ${quoteIdentifier(item.alias)}`),
    ...aggregations.map((aggregation, index) => aggregation.function === 'COUNT' && aggregation.expression === '*'
      ? `1 AS ${quoteIdentifier(`__r${index}`)}`
      : `${aggregation.expression} AS ${quoteIdentifier(`__r${index}`)}`)
  ]
  const workerSqlUncombined = aggregations.length
    ? `SELECT ${rawSelectSql.join(', ')} FROM ${quoteIdentifier(leftAlias)} INNER JOIN ${quoteIdentifier(rightAlias)} ON ${onLeft} = ${onRight}${whereSql}`
    : workerSql

  let orderByColumn = ''
  let orderByDirection = 'ASC'
  if (ast.orderby?.length) {
    if (ast.orderby.length !== 1 || ast.orderby[0].expr?.type !== 'column_ref' || ast.orderby[0].expr.table) {
      unsupported('joined ORDER BY must reference one output name')
    }
    orderByColumn = ast.orderby[0].expr.column
    orderByDirection = String(ast.orderby[0].type || 'ASC').toUpperCase()
  }
  let limit = 0
  if (ast.limit?.value?.length) limit = Number(ast.limit.value.at(-1).value)
  const outputs = [...projection.map(item => item.alias), ...aggregations.map(item => item.alias)]
  if (new Set(outputs).size !== outputs.length) unsupported('joined output names must be unique; use AS aliases')
  if (orderByColumn && !outputs.includes(orderByColumn)) unsupported('joined ORDER BY must appear in SELECT')

  return {
    tableName: left.table,
    tables: [
      { name: left.table, alias: leftAlias },
      { name: right.table, alias: rightAlias }
    ],
    join: {
      type: 'INNER',
      leftAlias: on.left.table,
      leftColumn: on.left.column,
      rightAlias: on.right.table,
      rightColumn: on.right.column
    },
    workerSql,
    workerSqlUncombined,
    columnReferences: projection.map(({ table, column }) => ({ table, column })),
    outputReferences: projection.map(({ table, column, alias }) => ({ table, column, alias })),
    aggregateReferences: aggregations
      .filter(item => item.column !== '*')
      .map(({ function: fn, table, column }) => ({ function: fn, table, column })),
    selectColumns: projection.map(item => item.alias),
    predicates,
    groupByColumns: groupBy.map(item => item.alias),
    aggregations: aggregations.map(({ function: fn, column, alias }) => ({ function: fn, column, alias })),
    orderByColumn,
    orderByDirection,
    orderByType: aggregations.some(item => item.alias === orderByColumn) ? 'number' : 'string',
    limit
  }
}

/**
 * Recursively walk a WHERE AST node and extract flat predicate list.
 * Supports AND-joined conditions only (per spec).
 *
 * @param {object|null} whereNode
 * @returns {Array<{column, operator, value, type}>}
 */
function extractPredicates (whereNode) {
  if (!whereNode) return []

  // AND node — recurse both sides
  if (whereNode.type === 'binary_expr' && String(whereNode.operator).toUpperCase() === 'AND') {
    return [
      ...extractPredicates(whereNode.left),
      ...extractPredicates(whereNode.right)
    ]
  }

  // Comparison operator
  if (whereNode.type === 'binary_expr') {
    const op      = whereNode.operator  // >, <, =, >=, <=, !=
    const left    = whereNode.left
    const right   = whereNode.right

    if (!SUPPORTED_COMPARISON_OPERATORS.has(op)) {
      unsupported(`WHERE operator ${op}; only AND-joined comparisons are supported`)
    }

    // column op literal
    if (left.type === 'column_ref' && right.type === 'number') {
      return [{
        column:   left.column,
        operator: op,
        value:    String(right.value),
        type:     'number'
      }]
    }

    if (left.type === 'column_ref' && right.type === 'single_quote_string') {
      return [{
        column:   left.column,
        operator: op,
        value:    right.value,
        type:     'string'
      }]
    }

    // Also handle double-quoted or unquoted string literals
    if (left.type === 'column_ref' && (right.type === 'string' || right.type === 'double_quote_string')) {
      return [{
        column:   left.column,
        operator: op,
        value:    right.value,
        type:     'string'
      }]
    }
  }

  unsupported('WHERE predicates must compare a column with a string or numeric literal')
}

/**
 * Extract aggregation functions from the SELECT columns array.
 *
 * @param {Array} columns  - ast.columns
 * @returns {Array<{function, column, alias}>}
 */
function extractAggregations (columns) {
  if (!columns || columns === '*') return []

  const aggs = []
  for (const col of columns) {
    if (col.expr && col.expr.type === 'aggr_func') {
      const fn = col.expr.name.toUpperCase()  // COUNT, SUM, AVG
      if (!SUPPORTED_AGGREGATIONS.has(fn)) unsupported(`aggregate function ${fn}`)
      if (col.expr.args?.distinct) unsupported('DISTINCT aggregates')
      let column = '*'
      if (col.expr.args && col.expr.args.expr) {
        const argExpr = col.expr.args.expr
        if (argExpr.type !== 'star' && argExpr.type !== 'column_ref') {
          unsupported(`${fn} arguments must be a column or *`)
        }
        column = argExpr.type === 'star' ? '*' : argExpr.column
      }
      if (column === '*' && fn !== 'COUNT') unsupported(`${fn}(*)`)
      aggs.push({
        function: fn,
        column,
        alias: col.as || `${fn.toLowerCase()}_${column}`
      })
    }
  }
  return aggs
}

/**
 * Extract plain (non-aggregate) column names from SELECT.
 *
 * @param {Array|string} columns - ast.columns
 * @returns {string[]}  - column names, or ['*'] for SELECT *
 */
function extractSelectColumns (columns) {
  if (!columns || columns === '*') return ['*']

  return columns
    .filter(col => col.expr && col.expr.type === 'column_ref')
    .map(col => col.expr.column)
}

function validateSelectColumns (columns) {
  if (columns === '*') return
  if (!Array.isArray(columns) || columns.length === 0) unsupported('empty SELECT list')

  for (const col of columns) {
    if (!col.expr || !['column_ref', 'aggr_func'].includes(col.expr.type)) {
      unsupported('SELECT expressions must be columns or COUNT/SUM/AVG/MIN/MAX aggregates')
    }
    if (col.expr.type === 'column_ref' && col.as) unsupported('aliases on non-aggregate columns')
  }
}

function validatePlanAgainstSchema (plan, schemaJson) {
  const schema = typeof schemaJson === 'string' ? JSON.parse(schemaJson) : schemaJson
  const columns = Array.isArray(schema?.columns) ? schema.columns : []
  const types = new Map(columns.map(column => [column.name, column.type]))
  const requireColumn = (column) => {
    if (column !== '*' && !types.has(column)) throw new Error(`Unknown column: ${column}`)
  }

  for (const column of plan.selectColumns) requireColumn(column)
  for (const column of plan.groupByColumns) requireColumn(column)
  for (const predicate of plan.predicates) {
    requireColumn(predicate.column)
    const columnType = types.get(predicate.column)
    if (columnType && columnType !== predicate.type) {
      throw new Error(`Predicate type mismatch for ${predicate.column}: expected ${columnType}`)
    }
  }
  for (const aggregation of plan.aggregations) {
    requireColumn(aggregation.column)
    if (aggregation.function !== 'COUNT' && types.get(aggregation.column) !== 'number') {
      throw new Error(`${aggregation.function} requires a numeric column: ${aggregation.column}`)
    }
  }

  const outputColumns = new Set([
    ...plan.selectColumns.filter(column => column !== '*'),
    ...plan.groupByColumns,
    ...plan.aggregations.map(aggregation => aggregation.alias)
  ])
  if (plan.selectColumns.includes('*')) {
    for (const column of types.keys()) outputColumns.add(column)
  }
  if (plan.orderByColumn && !outputColumns.has(plan.orderByColumn)) {
    throw new Error(`ORDER BY column must appear in the result: ${plan.orderByColumn}`)
  }

  const duplicateOutputs = [...plan.groupByColumns, ...plan.aggregations.map(aggregation => aggregation.alias)]
  if (new Set(duplicateOutputs).size !== duplicateOutputs.length) {
    throw new Error('Output column names and aggregate aliases must be unique')
  }

  const aggregate = plan.aggregations.find(item => item.alias === plan.orderByColumn)
  plan.orderByType = aggregate ? 'number' : types.get(plan.orderByColumn) || 'string'
  return plan
}

function validateJoinPlanAgainstSchemas (plan, datasets) {
  if (!plan.join || !Array.isArray(plan.tables)) throw new Error('Join plan is required')
  const byName = new Map(datasets.map(dataset => [String(dataset.name).toLowerCase(), dataset]))
  const schemas = new Map()
  for (const table of plan.tables) {
    const dataset = byName.get(String(table.name).toLowerCase())
    if (!dataset) throw new Error(`Dataset for joined table ${table.name} was not provided`)
    const schema = typeof dataset.schema_json === 'string' ? JSON.parse(dataset.schema_json) : dataset.schema_json
    schemas.set(table.alias, new Map((schema?.columns || []).map(column => [column.name, column.type])))
  }
  const requireColumn = (table, column) => {
    const columns = schemas.get(table)
    if (!columns?.has(column)) throw new Error(`Unknown joined column: ${table}.${column}`)
    return columns.get(column)
  }
  const leftType = requireColumn(plan.join.leftAlias, plan.join.leftColumn)
  const rightType = requireColumn(plan.join.rightAlias, plan.join.rightColumn)
  if (leftType !== rightType) throw new Error(`Join key type mismatch: ${leftType} versus ${rightType}`)
  for (const reference of plan.columnReferences) requireColumn(reference.table, reference.column)
  for (const predicate of plan.predicates) {
    const type = requireColumn(predicate.table, predicate.column)
    if (type !== predicate.type) throw new Error(`Predicate type mismatch for ${predicate.table}.${predicate.column}`)
  }
  for (const aggregation of plan.aggregateReferences) {
    const type = requireColumn(aggregation.table, aggregation.column)
    if (aggregation.function !== 'COUNT' && type !== 'number') {
      throw new Error(`${aggregation.function} requires a numeric column: ${aggregation.table}.${aggregation.column}`)
    }
  }
  if (plan.orderByColumn && !plan.aggregations.some(item => item.alias === plan.orderByColumn)) {
    const output = plan.outputReferences.find(item => item.alias === plan.orderByColumn)
    if (output) plan.orderByType = requireColumn(output.table, output.column)
  }
  return plan
}

/**
 * Parse SQL and return a structured execution plan.
 *
 * @param {string} sql
 * @returns {{
 *   tableName: string,
 *   selectColumns: string[],
 *   predicates: object[],
 *   groupByColumns: string[],
 *   aggregations: object[],
 *   orderByColumn: string,
 *   orderByDirection: string,
 *   limit: number
 * }}
 */
function buildExecutionPlan (sql) {
  const span = tracer.startSpan('query.plan', {
    attributes: { 'sql': sql.slice(0, 200) }
  })

  try {
    let ast
    try {
      ast = parser.astify(sql, { database: 'PostgreSQL' })
    } catch (err) {
      throw new Error(`SQL parse error: ${err.message}`)
    }

    if (Array.isArray(ast)) {
      if (ast.length !== 1) unsupported('multiple statements')
      ast = ast[0]
    }

    if (!ast || ast.type !== 'select') {
      throw new Error('Only SELECT statements are supported')
    }

    // ── Table name ──────────────────────────────────────────────────────────────
    if (!ast.from || ast.from.length === 0) {
      throw new Error('SELECT must specify a FROM table')
    }
    const isJoin = ast.from.length === 2 && Boolean(ast.from[1].join)
    if (!isJoin && (ast.from.length !== 1 || ast.from[0].join || !ast.from[0].table || ast.from[0].expr)) {
      unsupported('multiple FROM sources and subqueries')
    }
    if (ast.with) unsupported('common table expressions')
    if (ast.distinct?.type) unsupported('SELECT DISTINCT')
    if (ast.having) unsupported('HAVING')
    if (ast.union || ast._next) unsupported('UNION/INTERSECT/EXCEPT')
    if (isJoin) return buildJoinPlan(ast)
    const tableName = ast.from[0].table

    // ── Predicates (WHERE pushdown) ─────────────────────────────────────────────
    const predicates = extractPredicates(ast.where)

    // ── Aggregations ─────────────────────────────────────────────────────────────
    validateSelectColumns(ast.columns)
    const aggregations = extractAggregations(ast.columns)

    // ── Plain select columns (excluding aggr_func columns) ──────────────────────
    const selectColumns = extractSelectColumns(ast.columns)

    // ── GROUP BY ─────────────────────────────────────────────────────────────────
    const groupByNodes = Array.isArray(ast.groupby) ? ast.groupby : (ast.groupby?.columns || [])
    const groupByColumns = groupByNodes.length > 0
      ? groupByNodes.map(g => g.column || g.expr?.column).filter(Boolean)
      : []
    if (groupByNodes.some(g => !(g.column || g.expr?.column))) {
      unsupported('GROUP BY expressions must be plain columns')
    }

    if (aggregations.length > 0) {
      const grouped = new Set(groupByColumns)
      const ungrouped = selectColumns.filter(column => !grouped.has(column))
      if (ungrouped.length > 0) {
        unsupported(`selected columns must appear in GROUP BY: ${ungrouped.join(', ')}`)
      }
    } else if (groupByColumns.length > 0) {
      unsupported('GROUP BY without an aggregate')
    }

    // ── ORDER BY ─────────────────────────────────────────────────────────────────
    let orderByColumn    = ''
    let orderByDirection = 'ASC'
    if (ast.orderby && ast.orderby.length > 0) {
      if (ast.orderby.length !== 1 || ast.orderby[0].expr?.type !== 'column_ref') {
        unsupported('ORDER BY must contain one plain column or output alias')
      }
      const ob = ast.orderby[0]
      orderByColumn    = ob.expr?.column || ''
      orderByDirection = (ob.type || 'ASC').toUpperCase()
      if (!['ASC', 'DESC'].includes(orderByDirection)) unsupported(`ORDER BY direction ${orderByDirection}`)
    }

    // ── LIMIT ─────────────────────────────────────────────────────────────────────
    let limit = 0
    if (ast.limit && (!Array.isArray(ast.limit.value) || ast.limit.value.length > 0)) {
      const limitVal = ast.limit.value
      if (Array.isArray(limitVal) && limitVal.length > 0) {
        limit = parseInt(limitVal[limitVal.length - 1].value, 10) || 0
      } else if (typeof limitVal === 'number') {
        limit = limitVal
      }
      if (!Number.isSafeInteger(limit) || limit < 1) unsupported('LIMIT must be a positive integer')
    }

    span.setAttributes({
      'query.table':        tableName,
      'query.predicates':   predicates.length,
      'query.aggregations': aggregations.length,
      'query.group_by':     groupByColumns.length
    })

    return {
      tableName,
      selectColumns,
      predicates,
      groupByColumns,
      aggregations,
      orderByColumn,
      orderByDirection,
      limit
    }
  } catch (err) {
    span.recordException(err)
    throw err
  } finally {
    span.end()
  }
}

module.exports = { buildExecutionPlan, validatePlanAgainstSchema, validateJoinPlanAgainstSchemas }
