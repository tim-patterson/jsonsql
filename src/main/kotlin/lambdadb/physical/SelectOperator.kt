package lambdadb.physical

import lambdadb.ast.Ast

class SelectOperator(val source: Operator, val expressions: List<Ast.NamedExpr>): Operator() {
    private lateinit var compiledColumnAliases: List<String>
    private lateinit var compiledExpressions: List<ExpressionExecutor>

    override fun columnAliases() = compiledColumnAliases

    override fun compile() {
        source.compile()

        var colIdx = 0
        compiledColumnAliases = expressions.map { expression ->
            when {
                expression.alias != null -> expression.alias
                expression.expression is Ast.Expression.Identifier -> expression.expression.identifier
                else -> "col_${colIdx++}"
            }
        }

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

