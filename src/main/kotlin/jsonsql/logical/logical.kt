package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.safe

sealed class LogicalOperator {
    abstract fun fields(): List<String>
    abstract fun children(): List<LogicalOperator>

    data class Project(var expressions: List<Ast.NamedExpr>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = expressions.map { it.alias!! }
        override fun children() = listOf(sourceOperator)
    }

    data class LateralView(var expression: Ast.NamedExpr, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = (sourceOperator.fields() + expression.alias!!).distinct()
        override fun children() = listOf(sourceOperator)
    }

    data class Filter(var predicate: Ast.Expression, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
        override fun children() = listOf(sourceOperator)
    }

    data class Sort(var sortExpressions: List<Ast.OrderExpr>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
        override fun children() = listOf(sourceOperator)
    }

    data class Limit(var limit: Int, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
        override fun children() = listOf(sourceOperator)
    }

    data class Describe(var tableDefinition: Ast.Table): LogicalOperator() {
        override fun fields() = listOf("column_name", "column_type")
        override fun children() = listOf<LogicalOperator>()
    }

    data class DataSource(var fields: List<String>, var tableDefinition: Ast.Table): LogicalOperator() {
        override fun fields() = fields
        override fun children() = listOf<LogicalOperator>()
    }

    data class Explain(var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = listOf("plan")
        override fun children() = listOf(sourceOperator)
    }

    data class GroupBy(var expressions: List<Ast.NamedExpr>, var groupByExpressions: List<Ast.Expression>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = expressions.map { it.alias!! }
        override fun children() = listOf(sourceOperator)
    }

    data class Gather(var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields()
        override fun children() = listOf(sourceOperator)
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
    parallelize(tree)
    return tree
}

private fun fromSource(node: Ast.Source): LogicalOperator {
    return when(node) {
        is Ast.Source.Table -> fromTable(node.table)
        is Ast.Source.InlineView -> fromSelect(node.inlineView)
        is Ast.Source.LateralView -> LogicalOperator.LateralView(node.expression, fromSource(node.source))
    }
}


private fun fromSelect(node: Ast.Statement.Select): LogicalOperator {
    var operator = fromSource(node.source)

    if (node.predicate != null) {
        operator = LogicalOperator.Filter(node.predicate, operator)
    }

    // if we've got aggregation functions in the select but no group by, its really still a group by
    if (node.groupBy != null || node.expressions.map { checkForAggregate(it.expression) }.any { it } ) {
        val groupByKeys = node.groupBy ?: listOf()

        operator = LogicalOperator.GroupBy(node.expressions, groupByKeys, operator)
    } else {
        operator = LogicalOperator.Project(node.expressions, operator)
    }

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

        is LogicalOperator.GroupBy -> {
            // Make sure all columns are named
            operator.expressions = operator.expressions.mapIndexed { index, expr ->
                val alias = expr.alias ?: if(expr.expression is Ast.Expression.Identifier) expr.expression.identifier else "_col$index"
                Ast.NamedExpr(expr.expression, alias)
            }

            val allexprs = operator.groupByExpressions + operator.expressions.map { it.expression }

            val upstreamFieldsNeeded = allexprs.flatMap { neededFields(it) }.distinct()

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

        is LogicalOperator.LateralView -> {
            val upstreamFieldsNeeded = (neededFields(operator.expression.expression) + neededFields).distinct()
            val expr = operator.expression.expression
            val alias = operator.expression.alias ?: if(expr is Ast.Expression.Identifier) expr.identifier else null
            semanticAssert(alias != null, "Lateral View must have alias")
            operator.expression = operator.expression.copy(alias = alias)
            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }
        is LogicalOperator.DataSource -> operator.fields = neededFields
        else -> operator.children().map { populateFields(it) }
    }
}

private fun checkForAggregate(expr: Ast.Expression) : Boolean {
    return if(expr is Ast.Expression.Function) {
        semanticAssert(expr.functionName in functionRegistry, "function \"${expr.functionName}\" not found")
        if (functionRegistry[expr.functionName]!! is Function.AggregateFunction) {
            true
        } else {
            expr.parameters.map { checkForAggregate(it) }.any { it }
        }
    } else {
        false
    }
}


private fun neededFields(expression: Ast.Expression): List<String> {
    return when (expression) {
        is Ast.Expression.Identifier -> listOf(expression.identifier)
        is Ast.Expression.Constant -> listOf()
        is Ast.Expression.Function -> expression.parameters.flatMap(::neededFields)
    }.distinct()
}