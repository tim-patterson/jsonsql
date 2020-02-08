package jsonsql.physical.operators

import jsonsql.query.Expression
import jsonsql.functions.BooleanInspector
import jsonsql.physical.*

class FilterOperator(
        private val predicate: Expression,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledExpression = compileExpression(predicate, source.columnAliases)
        val sourceData = source.data(context)
        return sourceData.filter { sourceRow ->
            val result = compiledExpression.evaluate(sourceRow)

            BooleanInspector.inspect(result) == true
        }.withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Filter(${predicate})"
}

