package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry

fun validate(tree: LogicalTree) = ExpressionValidator.visit(tree, Unit)


private object ExpressionValidator: LogicalVisitor<Unit>() {

    override fun visit(expression: Ast.Expression.Identifier, operator: LogicalOperator, context: Unit): Ast.Expression {
        val field = expression.field
        val sourceFields = operator.children.flatMap { it.fields() }
        if (field.tableAlias != null) {
            semanticAssert(field in sourceFields, "$field not found in $sourceFields")
        } else {
            val matchCount = sourceFields.count { it.fieldName == field.fieldName }

            semanticAssert(matchCount > 0, "$field not found in $sourceFields")
            semanticAssert(matchCount == 1, "$field is ambiguous in  $sourceFields")
        }

        return expression
    }

    override fun visit(expression: Ast.Expression.Function, operator: LogicalOperator, context: Unit): Ast.Expression {
        val function = functionRegistry[expression.functionName] ?: error("function \"${expression.functionName}\" not found")
        val sourceFields = operator.children.flatMap { it.fields() }

        if (operator !is LogicalOperator.GroupBy) {
            semanticAssert(function is Function.ScalarFunction, "function \"${expression.functionName}\" - aggregate functions not allowed here")
        }
        val paramCount = expression.parameters.size
        semanticAssert(function.validateParameterCount(paramCount), "function \"${expression.functionName}\" not valid for $paramCount parameters")

        return super.visit(expression, operator, context)
    }

}


fun semanticAssert(condition: Boolean, msg: String){
    if (!condition) {
        error(msg)
    }
}