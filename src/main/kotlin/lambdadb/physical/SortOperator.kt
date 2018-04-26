package lambdadb.physical

import lambdadb.ast.Ast

class SortOperator(val source: Operator, val sortExpressions: List<Ast.OrderExpr>): Operator() {
    private val sortedBuffer: Iterator<List<Any?>> by lazy(::sort)
    private lateinit var compiledSortBy: List<CompiledOrderByExpr>

    override fun columnAliases() = source.columnAliases()

    override fun compile() {
        source.compile()
        compiledSortBy = sortExpressions.map {
            CompiledOrderByExpr(compileExpression(it.expression, source.columnAliases()), if(it.asc) 1 else -1)
        }
    }

    override fun next(): List<Any?>? {
        if (sortedBuffer.hasNext()) {
            return sortedBuffer.next()
        } else {
            return null
        }
    }

    override fun close() {
        source.close()
    }

    private fun sort(): Iterator<List<Any?>> {
        val buffer = mutableListOf<List<Any?>>()
        while (true) {
            val row = source.next()
            row ?: break
            buffer.add(row)
        }

        buffer.sortWith(Comparator{ row1, row2 ->
            for (orderExpr in compiledSortBy) {
                val expr = orderExpr.expression
                val comparison = compare(expr.evaluate(row1), expr.evaluate(row2))
                if (comparison != 0) return@Comparator comparison * orderExpr.order
            }
            0
        })

        return buffer.iterator()
    }

    private fun compare(val1: Any?, val2: Any?): Int {
        if (val1 == val2) return 0
        if (val1 == null) return -1
        if (val2 == null) return 1

        if (val1 is Number && val2 is Number) return val1.toDouble().compareTo(val2.toDouble())

        return val1.toString().compareTo(val2.toString())
    }

    // For explain output
    override fun toString() = "Sort($sortExpressions)"
    override fun children() = listOf(source)
}

private data class CompiledOrderByExpr(val expression: ExpressionExecutor, val order: Int)