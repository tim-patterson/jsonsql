package jsonsql.physical.operators

import jsonsql.query.Field
import jsonsql.functions.ArrayInspector
import jsonsql.physical.*
import jsonsql.query.NamedExpr

class LateralViewOperator(
        private val expression: NamedExpr,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy {
        // If we end up shadowing the parent var then we want to pluck out the shadowed field
        source.columnAliases.filterNot { it.fieldName == expression.alias } + listOf(Field(null, expression.alias!!))
    }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledExpression = compileExpression(expression.expression, source.columnAliases)
        val shadowedIndex = source.columnAliases.indexOfFirst { it.fieldName == expression.alias }
        val sourceData = source.data(context)

        val seq = sourceData.flatMap { parentRow ->
            val array = ArrayInspector.inspect(compiledExpression.evaluate(parentRow))

            val shadowed = if (shadowedIndex == -1) {
                parentRow
            } else {
                parentRow.take(shadowedIndex) + parentRow.drop(shadowedIndex + 1)
            }
            array.orEmpty().asSequence().map { shadowed + it }
        }

        return seq.withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "LateralView(${expression})"
}

