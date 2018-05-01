package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.ArrayInspector
import jsonsql.physical.ExpressionExecutor
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.compileExpression

class LateralViewOperator(val expression: Ast.NamedExpr, val source: PhysicalOperator): PhysicalOperator() {
    private lateinit var compiledExpression: ExpressionExecutor
    private var shadowedFieldIdx: Int = -1
    private var subViewIter : Iterator<List<Any?>> = listOf<List<Any?>>().iterator()

    override fun columnAliases(): List<Field> {
        // Shadowing implemented  here as it seems like it will be a common case
        return source.columnAliases().filterNot { it.fieldName ==  expression.alias } + Field(null, expression.alias!!)
    }

    override fun compile() {
        source.compile()
        shadowedFieldIdx = source.columnAliases().indexOfFirst { it.fieldName == expression.alias }
        compiledExpression = compileExpression(expression.expression, source.columnAliases())
    }

    override fun next(): List<Any?>? {
        while(true) {
            if (subViewIter.hasNext()) {
                return subViewIter.next()
            }
            val sourceRow = source.next()
            sourceRow ?: return null // terminal

            val subview = ArrayInspector.inspect(compiledExpression.evaluate(sourceRow))
            subview ?: continue
            val sourceRowWithoutShadow = sourceRow.toMutableList()
            if (shadowedFieldIdx != -1) sourceRowWithoutShadow.removeAt(shadowedFieldIdx)
            subViewIter = subview.map { sourceRowWithoutShadow + it }.iterator()
        }

    }

    override fun close() {
        source.close()
    }

    // For explain output
    override fun toString() = "Project(${expression})"
    override fun children() = listOf(source)
}

