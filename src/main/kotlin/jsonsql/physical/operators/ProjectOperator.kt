package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.ExpressionExecutor
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.compileExpressions

class ProjectOperator(val expressions: List<Ast.NamedExpr>, val source: PhysicalOperator, val tableAlias: String?): PhysicalOperator() {
    private lateinit var compiledExpressions: List<ExpressionExecutor>

    override fun columnAliases() = expressions.map { Field(tableAlias, it.alias!!) }

    override fun compile() {
        source.compile()

        compiledExpressions = compileExpressions(expressions.map(Ast.NamedExpr::expression), source.columnAliases())
    }

    override fun next(): List<Any?>? {
        val sourceRow =  source.next()
        sourceRow?: return null

        return compiledExpressions.map { it.evaluate(sourceRow) }
    }

    override fun close() {
        source.close()
    }

    // For explain output
    override fun toString() = "Select(${expressions})"
    override fun children() = listOf(source)
}

