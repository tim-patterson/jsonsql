package lambdadb.logical

import lambdadb.ast.Ast
import lambdadb.functions.scalarfunctionRegistry
import lambdadb.safe

fun validate(operator: LogicalOperator) {
    when(operator) {
        is LogicalOperator.Project -> {
            operator.expressions.map { validateExpression(it.expression, operator.sourceOperator.fields()) }
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
    }.safe
}

private fun validateExpression(expression: Ast.Expression, sourceFields: List<String>) {
    when(expression) {
        is Ast.Expression.Constant -> null
        is Ast.Expression.Identifier -> {semanticAssert(expression.identifier in sourceFields, "${expression.identifier} not found in $sourceFields"); null}
        is Ast.Expression.Function -> {
            semanticAssert(expression.functionName in scalarfunctionRegistry, "function \"${expression.functionName}\" not found")
            val function = scalarfunctionRegistry[expression.functionName]!!
            val paramCount = expression.parameters.size
            semanticAssert(function.validateParameterCount(paramCount), "function \"${expression.functionName}\" not valid for $paramCount parameters")
            expression.parameters.map { validateExpression(it, sourceFields) }
        }
    }.safe
}

fun semanticAssert(condition: Boolean, msg: String){
    if (!condition) {
        throw SemanticValidationError(msg)
    }
}

class SemanticValidationError(msg: String): Exception(msg)