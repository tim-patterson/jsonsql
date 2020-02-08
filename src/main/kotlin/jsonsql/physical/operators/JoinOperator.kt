package jsonsql.physical.operators

import jsonsql.functions.BooleanInspector
import jsonsql.physical.*
import jsonsql.query.Expression

class JoinOperator(
        private val joinCondition: Expression,
        private val left: PhysicalOperator,
        private val right: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { left.columnAliases + right.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledExpression = compileExpression(joinCondition, left.columnAliases + right.columnAliases)

        val leftData = left.data(context)
        var smallTable: List<Tuple>?
        return lazySeq {
            smallTable = right.data(context).use { rightData -> rightData.toList() }

            leftData.flatMap { bigTableRow ->
                smallTable!!.asSequence().map { bigTableRow + it }
            }.filter { BooleanInspector.inspect(compiledExpression.evaluate(it)) == true }
        }.withClose {
            smallTable = null // Free up heap
            leftData.close()
        }

    }

    // For explain output
    override fun toString() = "Join(${joinCondition})"
}

