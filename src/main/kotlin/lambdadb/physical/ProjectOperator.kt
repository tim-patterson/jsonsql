package lambdadb.physical

import lambdadb.ast.Ast

class ProjectOperator(val expressions: List<Ast.NamedExpr>, val source: PhysicalOperator): PhysicalOperator() {
    private lateinit var compiledExpressions: List<ExpressionExecutor>

    override fun columnAliases() = expressions.map { it.alias!! }

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

