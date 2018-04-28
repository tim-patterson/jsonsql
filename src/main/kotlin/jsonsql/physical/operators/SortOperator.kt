package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.physical.ExpressionExecutor
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.compileExpression

class SortOperator(val sortExpressions: List<Ast.OrderExpr>, val source: PhysicalOperator): PhysicalOperator() {
    private var buffer = mutableListOf<List<Any?>>()
    private val sortedBufferItr: Iterator<List<Any?>> by lazy(::sort)
    private lateinit var compiledSortBy: List<CompiledOrderByExpr>

    override fun columnAliases() = source.columnAliases()

    override fun compile() {
        source.compile()
        compiledSortBy = sortExpressions.map {
            CompiledOrderByExpr(compileExpression(it.expression, source.columnAliases()), if (it.asc) 1 else -1)
        }
    }

    override fun next(): List<Any?>? {
        return if (sortedBufferItr.hasNext()) {
            sortedBufferItr.next()
        } else {
            // Allow old buffer to be GC'd
            buffer = mutableListOf()
            null
        }
    }

    override fun close() {
        source.close()
    }

    private fun sort(): Iterator<List<Any?>> {
        while (true) {
            val row = source.next()
            row ?: break
            buffer.add(row)
        }

        buffer.sortWith(Comparator{ row1, row2 ->
            for (orderExpr in compiledSortBy) {
                val expr = orderExpr.expression
                val comparison = jsonsql.functions.compareValues(expr.evaluate(row1), expr.evaluate(row2))
                if (comparison != 0) return@Comparator comparison * orderExpr.order
            }
            0
        })

        return buffer.iterator()
    }

    // For explain output
    override fun toString() = "Sort($sortExpressions)"
    override fun children() = listOf(source)
}

private data class CompiledOrderByExpr(val expression: ExpressionExecutor, val order: Int)