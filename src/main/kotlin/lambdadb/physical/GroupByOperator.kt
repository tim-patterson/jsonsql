package lambdadb.physical

import lambdadb.ast.Ast

class GroupByOperator(val source: Operator, val expressions: List<Ast.NamedExpr>, val groupbyKeys: List<Ast.NamedExpr>): Operator() {

    override fun columnAliases() = TODO()

    override fun compile() {
        source.compile()
    }

    override fun next(): List<Any?>? {
        val sourceRow =  source.next()
        sourceRow?: return null
        TODO()
    }

    override fun close() {
        source.close()
    }

    // For explain output
    override fun toString() = "GroupBy(${expressions})"
    override fun children() = listOf(source)
}

