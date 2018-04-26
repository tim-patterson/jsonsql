package lambdadb.logical

import lambdadb.ast.Ast
import lambdadb.safe

sealed class LogicalOperator {
    abstract fun fields(): List<String>

    data class Project(var expressions: List<Ast.NamedExpr>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = expressions.map { it.alias!! }
    }

    data class Filter(var predicate: Ast.Expression, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
    }

    data class Sort(var sortExpressions: List<Ast.OrderExpr>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
    }

    data class Limit(var limit: Int, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
    }

    data class Describe(var tableDefinition: Ast.Table): LogicalOperator() {
        override fun fields() = listOf("column_name", "column_type")
    }

    data class DataSource(var fields: List<String>, var tableDefinition: Ast.Table): LogicalOperator() {
        override fun fields() = fields
    }

    data class Explain(var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = listOf("plan")
    }
}


fun logicalOperatorTree(stmt: Ast.Statement) : LogicalOperator {
    val tree = when(stmt) {
        is Ast.Statement.Describe -> LogicalOperator.Describe(stmt.tbl)
        is Ast.Statement.Select -> fromSelect(stmt)
        is Ast.Statement.Explain -> LogicalOperator.Explain(fromSelect(stmt.select))
    }
    populateFields(tree)
    validate(tree)
    return tree
}


private fun fromSelect(node: Ast.Statement.Select): LogicalOperator {
    var operator = when(node.source) {
        is Ast.Source.Table -> fromTable(node.source.table)
        is Ast.Source.InlineView -> fromSelect(node.source.inlineView)
    }

    if (node.predicate != null) {
        operator = LogicalOperator.Filter(node.predicate, operator)
    }

    operator = LogicalOperator.Project(node.expressions, operator)

    if (node.orderBy != null) {
        operator = LogicalOperator.Sort(node.orderBy, operator)
    }

    if (node.limit != null) {
        operator = LogicalOperator.Limit(node.limit, operator)
    }
    return operator
}


private fun fromTable(node: Ast.Table): LogicalOperator.DataSource {
    return LogicalOperator.DataSource(listOf(), node)
}


private fun populateFields(operator: LogicalOperator, neededFields: List<String> = listOf()) {
    // assign to force complete pattern match
    when(operator) {
        is LogicalOperator.Project -> {
            // Make sure all columns are named
            operator.expressions = operator.expressions.mapIndexed { index, expr ->
                val alias = expr.alias ?: if(expr.expression is Ast.Expression.Identifier) expr.expression.identifier else "_col$index"
                Ast.NamedExpr(expr.expression, alias)
            }

            val upstreamFieldsNeeded = operator.expressions.flatMap { neededFields(it.expression) }.distinct()

            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }

        is LogicalOperator.Filter -> {
            val upstreamFieldsNeeded = (neededFields(operator.predicate) + neededFields).distinct()
            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }

        is LogicalOperator.Sort -> {
            val upstreamFieldsNeeded = (operator.sortExpressions.flatMap { neededFields(it.expression) } + neededFields).distinct()
            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }

        is LogicalOperator.Limit -> populateFields(operator.sourceOperator, neededFields)
        is LogicalOperator.Describe -> null
        is LogicalOperator.DataSource -> operator.fields = neededFields
        is LogicalOperator.Explain -> populateFields(operator.sourceOperator)
    }.safe
}


private fun neededFields(expression: Ast.Expression): List<String> {
    return when (expression) {
        is Ast.Expression.Identifier -> listOf(expression.identifier)
        is Ast.Expression.Constant -> listOf()
        is Ast.Expression.Function -> expression.parameters.flatMap(::neededFields)
    }.distinct()
}