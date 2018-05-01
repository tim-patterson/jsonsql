package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*

class GroupByOperator(val expressions: List<Ast.NamedExpr>, val groupByKeys: List<Ast.Expression>, val source: PhysicalOperator, val tableAlias: String?): PhysicalOperator() {
    private lateinit var compiledExpressions: List<AggregateExpressionExecutor>
    private lateinit var compiledGroupExpressions: List<ExpressionExecutor>

    override fun columnAliases() = expressions.map { Field(tableAlias, it.alias!!) }

    private var currentGroupKeys: List<Any?>? = null

    // When there are no group by keys and no rows we still expect a row of output
    // its not consistent, but that's sql for you
    private var dirty: Boolean = groupByKeys.isEmpty()

    override fun compile() {
        source.compile()
        compiledExpressions = compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases())
        compiledGroupExpressions = compileExpressions(groupByKeys, source.columnAliases())
    }

    // We iterate until we hit the starting key(s) for the next group
    // However as we've already consumed it we have to store it in
    // an ivar for the next group
    override fun next(): List<Any?>? {
        while (true) {
            val sourceRow = source.next()
            val groupKeys = sourceRow?.let { compiledGroupExpressions.map { it.evaluate(sourceRow) } }
            // Handle first group
            if (currentGroupKeys == null) {
                currentGroupKeys = groupKeys
            }

            if (currentGroupKeys != groupKeys && groupKeys != null) {
                // hit first row for the next group
                // emit, reset and consume
                val groupResult = compiledExpressions.map { it.getResult() }
                compiledExpressions.map { it.reset() }
                compiledExpressions.map { it.processRow(sourceRow) }
                currentGroupKeys = groupKeys
                dirty = true
                return groupResult

            } else if (sourceRow == null) {
                if (!dirty) return null
                // end of all results emit record
                dirty = false
                return compiledExpressions.map { it.getResult() }

            } else {
                // still in group, just consume
                compiledExpressions.map { it.processRow(sourceRow) }
            }

        }
    }

    override fun close() {
        source.close()
    }

    // For explain output
    override fun toString() = "GroupBy(${expressions}, groupby - ${groupByKeys})"
    override fun children() = listOf(source)
}

