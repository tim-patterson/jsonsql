package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.BooleanInspector
import jsonsql.physical.*

class FilterOperator(
        private val predicate: Ast.Expression,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(): ClosableSequence<Tuple> {
        val compiledExpression = compileExpression(predicate, source.columnAliases)
        val sourceData = source.data()
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

