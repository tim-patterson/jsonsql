package jsonsql.physical.operators

import jsonsql.query.Field
import jsonsql.physical.*
import jsonsql.query.NamedExpr

class ProjectOperator(
        private val expressions: List<NamedExpr>,
        private val source: PhysicalOperator,
        private val tableAlias: String?
): PhysicalOperator() {

    override val columnAliases = expressions.map { Field(tableAlias, it.alias!!) }

    private val compiledExpressions = compileExpressions(expressions.map(NamedExpr::expression), source.columnAliases)

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val sourceData = source.data(context)
        return sourceData.map { row ->
            compiledExpressions.map { it.evaluate(row) }
        }.withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Select(${expressions})"
}

