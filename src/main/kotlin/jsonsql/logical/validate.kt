package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.safe

fun validate(operator: LogicalOperator) {
    when(operator) {
        is LogicalOperator.Project -> {
            operator.expressions.map { validateExpression(it.expression, operator.sourceOperator.fields()) }
        }
        is LogicalOperator.LateralView -> {
            validateExpression(operator.expression.expression, operator.sourceOperator.fields())
        }
        is LogicalOperator.GroupBy -> {
            operator.expressions.map { validateExpression(it.expression, operator.sourceOperator.fields(), true) }
            operator.groupByExpressions.map { validateExpression(it, operator.sourceOperator.fields()) }
        }
        is LogicalOperator.Filter -> {
            validateExpression(operator.predicate, operator.sourceOperator.fields())
        }
        is LogicalOperator.Sort -> {
            operator.sortExpressions.map { validateExpression(it.expression, operator.sourceOperator.fields()) }
        }
    }
    operator.children.map { validate(it) }
}

private fun validateExpression(expression: Ast.Expression, sourceFields: List<Field>, allowAggregate: Boolean = false) {
    when(expression) {
        is Ast.Expression.Constant -> null
        is Ast.Expression.Identifier -> {
            val field = expression.field
            if (field.tableAlias != null) {
                semanticAssert(field in sourceFields, "$field not found in $sourceFields")
            } else {
                val matchCount = sourceFields.count { it.fieldName == field.fieldName }

                semanticAssert(matchCount > 0, "$field not found in $sourceFields")
                semanticAssert(matchCount == 1, "$field is ambiguous in  $sourceFields")
            }

            null
        }
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