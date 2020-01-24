package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.BooleanInspector
import jsonsql.physical.*

class JoinOperator(
        private val joinCondition: Ast.Expression,
        private val left: PhysicalOperator,
        private val right: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { left.columnAliases + right.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledExpression = compileExpression(joinCondition, left.columnAliases + right.columnAliases)

        val leftData = left.data(context)
        val rightData = right.data(context)

        var smallTable: List<Tuple>? = rightData.toList()

        return leftData.flatMap { bigTableRow ->
                    smallTable!!.asSequence().map { bigTableRow + it }
                }
                .filter { BooleanInspector.inspect(compiledExpression.evaluate(it)) == true }
                .withClose {
                    smallTable = null
                    leftData.close()
                    rightData.close()
                }

    }

    // For explain output
    override fun toString() = "Join(${joinCondition})"
}

