package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry

sealed class LogicalOperator {
    abstract fun fields(): List<Field>
    abstract fun children(): List<LogicalOperator>
    // An Alias can be set at a operator level (ie table, or subselect)
    var alias: String? = null
    // this is really aliases in scope for anything that uses this operator as a source
    open fun aliasesInScope(): List<String> = alias?.let { listOf(it) } ?: children().flatMap { it.aliasesInScope() }

    data class Project(var expressions: List<Ast.NamedExpr>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = expressions.map { Field(alias, it.alias!!) }
        override fun children() = listOf(sourceOperator)
    }

    data class LateralView(var expression: Ast.NamedExpr, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = sourceOperator.fields().filterNot { it.fieldName == expression.alias!!} + Field(alias, expression.alias!!)
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
        override fun fields() = listOf("column_name", "column_type").map { Field(alias, it) }
        override fun children() = listOf<LogicalOperator>()
    }

    data class DataSource(var fields: List<String>, var tableDefinition: Ast.Table): LogicalOperator() {
        override fun fields() = fields.map { Field(alias, it) }
        override fun children() = listOf<LogicalOperator>()
    }

    data class Explain(var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = listOf(Field(null, "plan"))
        override fun children() = listOf(sourceOperator)
    }

    data class GroupBy(var expressions: List<Ast.NamedExpr>, var groupByExpressions: List<Ast.Expression>, var sourceOperator: LogicalOperator): LogicalOperator() {
        override fun fields() = expressions.map { Field(alias, it.alias!!) }
        override fun children() = listOf(sourceOperator)
    }

    data class Join(var sourceOperator1: LogicalOperator, var sourceOperator2: LogicalOperator, var onClause: Ast.Expression): LogicalOperator() {
        override fun fields() = sourceOperator1.fields() + sourceOperator2.fields()
        override fun children() = listOf(sourceOperator1, sourceOperator2)
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
        is Ast.Source.Table -> fromTable(node.table).apply { alias = node.tableAlias }
        is Ast.Source.InlineView -> fromSelect(node.inlineView).apply { alias = node.tableAlias }
        is Ast.Source.LateralView -> {
            val source = fromSource(node.source)
            val aliases = source.aliasesInScope()
            val expr = (node.expression.copy(expression = normalizeIdentifiers(node.expression.expression, aliases)))
            LogicalOperator.LateralView(expr, source)
        }
        is Ast.Source.Join -> fromJoin(node)
    }
}


private fun fromJoin(node: Ast.Source.Join): LogicalOperator.Join {
    val sourceOperator1 = fromSource(node.source1)
    val sourceOperator2 = fromSource(node.source2)
    val aliases = sourceOperator1.aliasesInScope() + sourceOperator2.aliasesInScope()
    val expr = normalizeIdentifiers(node.joinCondition, aliases)
    return LogicalOperator.Join(sourceOperator1, sourceOperator2, expr)
}


private fun fromSelect(node: Ast.Statement.Select): LogicalOperator {
    var operator = fromSource(node.source)
    val aliases = operator.aliasesInScope()

    if (node.predicate != null) {
        val pred = normalizeIdentifiers(node.predicate, aliases)
        operator = LogicalOperator.Filter(pred, operator)
    }

    // if we've got aggregation functions in the select but no group by, its really still a group by
    if (node.groupBy != null || node.expressions.map { checkForAggregate(it.expression) }.any { it } ) {
        val groupByKeys = node.groupBy?.let { normalizeIdentifiers(it, aliases) } ?: listOf()

        operator = LogicalOperator.GroupBy(normalizeIdentifiersForNamed(node.expressions, aliases), groupByKeys, operator)
    } else {
        operator = LogicalOperator.Project(normalizeIdentifiersForNamed(node.expressions, aliases), operator)
    }

    if (node.orderBy != null) {
        val orderby = node.orderBy.map { it.copy(expression = normalizeIdentifiers(it.expression, aliases)) }
        operator = LogicalOperator.Sort(orderby, operator)
    }

    if (node.limit != null) {
        operator = LogicalOperator.Limit(node.limit, operator)
    }
    return operator
}


private fun fromTable(node: Ast.Table): LogicalOperator.DataSource {
    return LogicalOperator.DataSource(listOf(), node)
}


private fun populateFields(operator: LogicalOperator, neededFields: List<Field> = listOf()) {
    when(operator) {
        is LogicalOperator.Project -> {
            // Make sure all columns are named
            operator.expressions = operator.expressions.mapIndexed { index, expr ->
                val alias = expr.alias ?: if(expr.expression is Ast.Expression.Identifier) expr.expression.field.fieldName else "_col$index"
                Ast.NamedExpr(expr.expression, alias)
            }

            val upstreamFieldsNeeded = operator.expressions.flatMap { neededFields(it.expression) }.distinct()

            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }

        is LogicalOperator.GroupBy -> {
            // Make sure all columns are named
            operator.expressions = operator.expressions.mapIndexed { index, expr ->
                val alias = expr.alias ?: if(expr.expression is Ast.Expression.Identifier) expr.expression.field.fieldName else "_col$index"
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
            val alias = operator.expression.alias ?: if(expr is Ast.Expression.Identifier) expr.field.fieldName else null
            semanticAssert(alias != null, "Lateral View must have alias")
            operator.expression = operator.expression.copy(alias = alias)
            populateFields(operator.sourceOperator, upstreamFieldsNeeded)
        }
        is LogicalOperator.Join -> {
            val upstreamFieldsNeeded = (neededFields(operator.onClause) + neededFields).distinct()
            populateFields(operator.sourceOperator1, upstreamFieldsNeeded)
            populateFields(operator.sourceOperator2, upstreamFieldsNeeded)
        }
        is LogicalOperator.DataSource -> {
            operator.fields = neededFields.filter { it.tableAlias == null || it.tableAlias == operator.alias }.map { it.fieldName }.distinct()
        }
        else -> operator.children().forEach { populateFields(it) }
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


private fun neededFields(expression: Ast.Expression): List<Field> {
    return when (expression) {
        is Ast.Expression.Identifier -> listOf(expression.field)
        is Ast.Expression.Constant -> listOf()
        is Ast.Expression.Function -> expression.parameters.flatMap(::neededFields)
    }.distinct()
}



// Tablename.identifer will be wrongly represented as idx(Tablename, identifier)
// Tablename.identifer.subfield will be idx(Tablename, idx(identifier, subfield))
// This function is here to correct these.
private fun normalizeIdentifiers(expression: Ast.Expression, tableAliasesInScope: List<String>): Ast.Expression {
    return when(expression) {
        is Ast.Expression.Constant -> expression
        is Ast.Expression.Identifier -> expression
        is Ast.Expression.Function -> {
            if (expression.functionName == "idx" && expression.parameters.size == 2) {
                val arg1 = expression.parameters[0]
                val arg2 = expression.parameters[1]
                if (arg1 is Ast.Expression.Identifier && arg1.field.fieldName in tableAliasesInScope) {
                    // This is one we need to fix, child element should either be an constant or
                    // another idx function but this may not be the case if someone is manually using
                    // the idx something to do something weird
                    val tableAlias = arg1.field.fieldName
                    when(arg2) {
                        is Ast.Expression.Constant -> {
                            if (arg2.value is String) return Ast.Expression.Identifier(Field(tableAlias, arg2.value))
                        }
                        is Ast.Expression.Function -> {
                            if(arg2.functionName == "idx" && (arg2.parameters.first() is Ast.Expression.Constant)) {
                                val identifierConst = arg2.parameters.first() as Ast.Expression.Constant
                                val identifier = Ast.Expression.Identifier(Field(tableAlias, identifierConst.value as String))
                                val params = listOf(identifier) + arg2.parameters.drop(1)
                                return Ast.Expression.Function("idx", params)
                            }
                        }
                    }
                }
            }

            expression.copy(parameters = expression.parameters.map { normalizeIdentifiers(it, tableAliasesInScope) })
        }
    }
}

private fun normalizeIdentifiers(expressions: List<Ast.Expression>, tableAliasesInScope: List<String>): List<Ast.Expression> {
    return expressions.map { normalizeIdentifiers(it, tableAliasesInScope) }
}

private fun normalizeIdentifiersForNamed(expressions: List<Ast.NamedExpr>, tableAliasesInScope: List<String>): List<Ast.NamedExpr> {
    return expressions.map { it.copy(expression = normalizeIdentifiers(it.expression, tableAliasesInScope)) }
}