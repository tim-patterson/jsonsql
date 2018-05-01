package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.functions.BooleanInspector
import jsonsql.physical.ExpressionExecutor
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.compileExpression

class JoinOperator(val joinCondition: Ast.Expression, val source1: PhysicalOperator, val source2: PhysicalOperator): PhysicalOperator() {
    private lateinit var compiledExpression: ExpressionExecutor
    private var smallTable: List<List<Any?>>? = null
    private var currIter: Iterator<List<Any?>> = listOf<List<Any?>>().iterator()

    override fun columnAliases() = source1.columnAliases() + source2.columnAliases()

    override fun compile() {
        source1.compile()
        source2.compile()

        compiledExpression = compileExpression(joinCondition, columnAliases())
    }

    override fun next(): List<Any?>? {
        if (smallTable == null) smallTable = loadSmallTable()
        while (true) {
            if (currIter.hasNext()) return currIter.next()

            val s1Row = source1.next() ?: return null
            val nextRows = mutableListOf<List<Any?>>()
            smallTable!!.forEach { s2Row ->
                val row = s1Row + s2Row
                if (BooleanInspector.inspect(compiledExpression.evaluate(row)) == true) {
                    nextRows.add(row)
                }
            }
            currIter = nextRows.iterator()
        }
    }

    override fun close() {
        smallTable = null
        source1.close()
        source2.close()
    }

    private fun loadSmallTable(): List<List<Any?>> {
        val rows = mutableListOf<List<Any?>>()
        while (true) {
            val row = source2.next() ?: return rows
            rows.add(row)
        }
    }

    // For explain output
    override fun toString() = "Join(${joinCondition})"
    override fun children() = listOf(source1, source2)
}

