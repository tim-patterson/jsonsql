package jsonsql.logical

import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.Expression

object ExpressionOptimizer: LogicalVisitor<Unit>() {
    override fun visit(expression: Expression.Function, operator: LogicalOperator, context: Unit): Expression {
        // This is here to first get the visitor to walk down the function and optimize the sub-expressions first
        val functionExpression = super.visit(expression, operator, context) as Expression.Function

        if (functionExpression.parameters.all { it is Expression.Constant }) {
            val function = functionRegistry[functionExpression.functionName]
            if (function is Function.ScalarFunction) {
                val args = functionExpression.parameters.map { (it as Expression.Constant).value }
                return Expression.Constant(function.execute(args))
            }

        }

        return functionExpression
    }
}