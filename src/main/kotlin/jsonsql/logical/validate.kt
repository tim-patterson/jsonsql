package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.safe

fun validate(operator: LogicalOperator) {
    when(operator) {
        is LogicalOperator.Project -> {
            operator.expressions.map { validateExpression(it.expression, operator.sourceOperator.fields()) }
            validate(operator.sourceOperator)
        }
        is LogicalOperator.LateralView -> {
            validateExpression(operator.expression.expression, operator.sourceOperator.fields())
            validate(operator.sourceOperator)
        }
        is LogicalOperator.GroupBy -> {
            operator.expressions.map { validateExpression(it.expression, operator.sourceOperator.fields(), true) }
            operator.groupByExpressions.map { validateExpression(it, operator.sourceOperator.fields()) }

            validate(operator.sourceOperator)
        }
        is LogicalOperator.Filter -> {
            validateExpression(operator.predicate, operator.sourceOperator.fields())
            validate(operator.sourceOperator)
        }
        is LogicalOperator.Sort -> {
            operator.sortExpressions.map { validateExpression(it.expression, operator.sourceOperator.fields()) }
            validate(operator.sourceOperator)
        }
        is LogicalOperator.Limit -> validate(operator.sourceOperator)
        is LogicalOperator.Describe -> null
        is LogicalOperator.DataSource -> null
        is LogicalOperator.Explain -> validate(operator.sourceOperator)
        is LogicalOperator.Gather -> null
    }.safe
}

private fun validateExpression(expression: Ast.Expression, sourceFields: List<String>, allowAggregate: Boolean = false) {
    when(expression) {
        is Ast.Expression.Constant -> null
        is Ast.Expression.Identifier -> {semanticAssert(expression.identifier in sourceFields, "${expression.identifier} not found in $sourceFields"); null}
        is Ast.Expression.Function -> {
            semanticAssert(expression.functionName in functionRegistry, "function \"${expression.functionName}\" not found")
            val function = functionRegistry[expression.functionName]!!
            if (!allowAggregate) {
                semanticAssert(function is Function.ScalarFunction, "function \"${expression.functionName}\" - aggregate functions not allowed here")
            }
            val paramCount = expression.parameters.size
            semanticAssert(function.validateParameterCount(paramCount), "function \"${expression.functionName}\" not valid for $paramCount parameters")
            expression.parameters.map { validateExpression(it, sourceFields, allowAggregate) }
        }
    }.safe
}


fun semanticAssert(condition: Boolean, msg: String){
    if (!condition) {
        throw SemanticValidationError(msg)
    }
}

class SemanticValidationError(msg: String): Exception(msg)