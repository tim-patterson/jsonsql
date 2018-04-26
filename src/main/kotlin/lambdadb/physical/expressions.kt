package lambdadb.physical

import lambdadb.ast.Ast
import lambdadb.functions.ScalarFunction
import lambdadb.functions.scalarfunctionRegistry

fun compileExpressions(expressions: List<Ast.Expression>, columnAliases: List<String>): List<ExpressionExecutor> {
    return expressions.map { expression -> compileExpression(expression, columnAliases)}
}

fun compileExpression(expression: Ast.Expression, columnAliases: List<String>): ExpressionExecutor {
    return when(expression) {
        is Ast.Expression.Identifier -> IdentifierExecutor(columnAliases.indexOf(expression.identifier))
        is Ast.Expression.Constant -> ConstantExecutor(expression.value)
        is Ast.Expression.Function -> {
            val function = scalarfunctionRegistry[expression.functionName]!!
            FunctionExecutor(function, compileExpressions(expression.parameters, columnAliases))
        }
    }
}


abstract class ExpressionExecutor {
    abstract fun evaluate(row: List<Any?>): Any?
}

class ConstantExecutor(val constant: Any?): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = constant
}

class IdentifierExecutor(val offset: Int): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = row[offset]
}

class FunctionExecutor(val function: ScalarFunction, val args: List<ExpressionExecutor>): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = function.execute(args.map { it.evaluate(row) })
}