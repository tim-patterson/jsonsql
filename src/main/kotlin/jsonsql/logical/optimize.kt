package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry

object ExpressionOptimizer: LogicalVisitor<Unit>() {
    override fun visit(expression: Ast.Expression.Function, operator: LogicalOperator, context: Unit): Ast.Expression {
        // This is here to first get the visitor to walk down the function and optimize the sub-expressions first
        val functionExpression = super.visit(expression, operator, context) as Ast.Expression.Function

        if (functionExpression.parameters.all { it is Ast.Expression.Constant }) {
            val function = functionRegistry[functionExpression.functionName]
            if (function is Function.ScalarFunction) {
                val args = functionExpression.parameters.map { (it as Ast.Expression.Constant).value }
                return Ast.Expression.Constant(function.execute(args))
            }

        }

        return functionExpression
    }
}