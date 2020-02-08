package jsonsql.logical

import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.*
import jsonsql.query.validate.semanticAssert

fun logicalOperatorTree(query: Query) : LogicalTree {
    var tree = LogicalTree(fromQuery(query))
    tree = PopulateFieldsVisitor.visit(tree, setOf())
    return parallelize(tree)
}


private fun fromQuery(node: Query): LogicalOperator {
    return when(node) {
        is Query.Describe -> LogicalOperator.Describe(node.tbl, node.tableOutput)
        is Query.Select -> fromSelect(node)
        is Query.Explain -> LogicalOperator.Explain(fromQuery(node.query))
        is Query.Insert -> LogicalOperator.Write(node.tbl, fromQuery(node.query))
    }
}

private fun fromSource(node: Query.SelectSource): LogicalOperator {
    return when(node) {
        is Query.SelectSource.JustATable -> fromTable(node)
        // TODO point this at fromQuery instead once the alias stuff is sorted properly
        is Query.SelectSource.InlineView -> fromSelect(node.inner as Query.Select, node.tableAlias)
        is Query.SelectSource.LateralView -> {
            val source = fromSource(node.source)
            val expr = (node.expression.copy(expression = node.expression.expression))
            LogicalOperator.LateralView(expr, source)
        }
        is Query.SelectSource.Join -> fromJoin(node)
    }
}


private fun fromJoin(node: Query.SelectSource.Join): LogicalOperator.Join {
    val sourceOperator1 = fromSource(node.source1)
    val sourceOperator2 = fromSource(node.source2)
    val expr = node.joinCondition
    return LogicalOperator.Join(sourceOperator1, sourceOperator2, expr)
}


private fun fromSelect(node: Query.Select, tableAlias: String? = null ): LogicalOperator {
    var operator = fromSource(node.source)

    if (node.predicate != null) {
        operator = LogicalOperator.Filter(node.predicate, operator)
    }

    // if we've got aggregation functions in the select but no group by, its really still a group by
    if (node.groupBy != null || node.expressions.map { checkForAggregate(it.expression) }.any { it } ) {
        val groupByKeys = node.groupBy ?: listOf()

        operator = LogicalOperator.GroupBy(node.expressions, groupByKeys, operator, tableAlias)
    } else {
        operator = LogicalOperator.Project(node.expressions, operator, tableAlias)
    }

    if (node.orderBy != null) {
        operator = LogicalOperator.Sort(node.orderBy, operator)
    }

    if (node.limit != null) {
        operator = LogicalOperator.Limit(node.limit, operator)
    }
    return operator
}


private fun fromTable(node: Query.SelectSource.JustATable): LogicalOperator.DataSource {
    return LogicalOperator.DataSource(listOf(), node.table, node.tableAlias)
}


private fun checkForAggregate(expr: Expression) : Boolean {
    return if(expr is Expression.Function) {
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

/**
 * Pushes the list of required fields down to the table sources.
 */
private object PopulateFieldsVisitor: LogicalVisitor<Set<Field>>() {
    override fun visit(operator: LogicalOperator.Project, context: Set<Field>): LogicalOperator {
        return super.visit(operator, operator.expressions.flatMap { neededFields(it.expression) }.toSet() )
    }

    override fun visit(operator: LogicalOperator.GroupBy, context: Set<Field>): LogicalOperator {
        val allexprs = operator.groupByExpressions + operator.expressions.map { it.expression }
        return super.visit(operator, allexprs.flatMap { neededFields(it) }.toSet())
    }

    override fun visit(operator: LogicalOperator.Filter, context: Set<Field>): LogicalOperator {
        return super.visit(operator, context + neededFields(operator.predicate))
    }

    override fun visit(operator: LogicalOperator.Sort, context: Set<Field>): LogicalOperator {
        val upstreamFieldsNeeded = context + operator.sortExpressions.flatMap { neededFields(it.expression) }
        return super.visit(operator, upstreamFieldsNeeded)
    }

    override fun visit(operator: LogicalOperator.LateralView, context: Set<Field>): LogicalOperator {
        semanticAssert(operator.expression.alias != null, "Lateral View must have alias")
        val upstreamFieldsNeeded = neededFields(operator.expression.expression) + context
        return super.visit(operator, upstreamFieldsNeeded)
    }

    override fun visit(operator: LogicalOperator.Join, context: Set<Field>): LogicalOperator {
        return super.visit(operator, neededFields(operator.onClause) + context)
    }

    override fun visit(operator: LogicalOperator.DataSource, context: Set<Field>): LogicalOperator {
        val fields = context.filter {  it.tableAlias == null || it.tableAlias == operator.alias }
                .map { it.fieldName }.distinct()
        return operator.copy(fieldNames = fields)
    }

    private fun neededFields(expression: Expression): Set<Field> {
        return when (expression) {
            is Expression.Identifier -> setOf(expression.field)
            is Expression.Constant -> setOf()
            is Expression.Function -> expression.parameters.flatMap(::neededFields).toSet()
        }
    }
}
