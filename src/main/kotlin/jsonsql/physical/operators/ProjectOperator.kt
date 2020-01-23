package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*

class ProjectOperator(
        private val expressions: List<Ast.NamedExpr>,
        private val source: PhysicalOperator,
        private val tableAlias: String?
): PhysicalOperator() {

    override val columnAliases = expressions.map { Field(tableAlias, it.alias!!) }

    private val compiledExpressions = compileExpressions(expressions.map(Ast.NamedExpr::expression), source.columnAliases)

    override fun data(): ClosableSequence<Tuple> {
        val sourceData = source.data()
        return sourceData.map { row ->
            compiledExpressions.map { it.evaluate(row) }
        }.withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Select(${expressions})"
}

