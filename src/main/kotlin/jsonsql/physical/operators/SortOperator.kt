package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*

class SortOperator(
        private val sortExpressions: List<Ast.OrderExpr>,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledSortBy = sortExpressions.map {
            CompiledOrderByExpr(compileExpression(it.expression, source.columnAliases), if (it.asc) 1 else -1)
        }

        val sourceData = source.data(context)
        return sourceData.sortedWith(Comparator { row1, row2 ->
            for (orderExpr in compiledSortBy) {
                val expr = orderExpr.expression
                val comparison = jsonsql.functions.compareValuesForSort(expr.evaluate(row1), expr.evaluate(row2))
                if (comparison != 0) return@Comparator comparison * orderExpr.order
            }
            0
        }).withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Sort($sortExpressions)"
}

private data class CompiledOrderByExpr(val expression: ExpressionExecutor, val order: Int)