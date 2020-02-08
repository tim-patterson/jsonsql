package jsonsql.logical

import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.*

sealed class LogicalOperator(
        vararg val children: LogicalOperator
) {
    open val alias: String? = null // The table alias of this operator ie from (...) as foo, the foo here gets assigned to the inner project or group by.
    abstract val fields: List<Field> // The fields exposed by the operator

    data class Project(val expressions: List<NamedExpr>, val sourceOperator: LogicalOperator, override val alias: String?): LogicalOperator(sourceOperator) {
        override val fields by lazy { expressions.map { Field(alias, it.alias!!) } }
    }

    data class LateralView(val expression: NamedExpr, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields.filterNot { it.fieldName == expression.alias!!} + Field(alias, expression.alias!!) }
    }

    data class Filter(val predicate: Expression, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Sort(val sortExpressions: List<OrderExpr>, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Limit(val limit: Int, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Describe(val tableDefinition: Table, val tableOutput: Boolean): LogicalOperator() {
        override val fields = if(tableOutput) {
            listOf("table")
        } else {
            listOf("column_name", "column_type")
        }.map { Field(alias, it) }
    }

    data class DataSource(val fieldNames: List<String>, val tableDefinition: Table, override val alias: String?): LogicalOperator() {
        override val fields = fieldNames.map { Field(alias, it) }
    }

    data class Explain(val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields = listOf(Field(null, "plan"))
    }

    data class GroupBy(val expressions: List<NamedExpr>, val groupByExpressions: List<Expression>, val sourceOperator: LogicalOperator, override val alias: String?): LogicalOperator(sourceOperator) {
        override val fields by lazy {  expressions.map { Field(alias, it.alias!!) } }
    }

    data class Join(val sourceOperator1: LogicalOperator, val sourceOperator2: LogicalOperator, val onClause: Expression): LogicalOperator(sourceOperator1, sourceOperator2) {
        override val fields  by lazy { sourceOperator1.fields + sourceOperator2.fields }
    }

    data class Gather(val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Write(val tableDefinition: Table, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { listOf(Field(null, "result")) }
    }
}

data class LogicalTree(val root: LogicalOperator)


fun logicalOperatorTree(query: Query) : LogicalTree {
    var tree = LogicalTree(fromQuery(query))
    tree = NormalizeIdentifiersVisitor.visit(tree, Unit)
    tree = PopulateFieldsVisitor.visit(tree, setOf())
    tree = validate(tree)
    tree = ExpressionOptimizer.visit(tree, Unit)
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
        is Query.SelectSource.InlineView -> fromSelect(node.inlineView as Query.Select, node.tableAlias)
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
 * Ensures all named expressions have aliases, and pushes the list of required fields down to the table sources.
 */
private object PopulateFieldsVisitor: LogicalVisitor<Set<Field>>() {
    override fun visit(namedExpression: NamedExpr, index: Int, operator: LogicalOperator, context: Set<Field>): NamedExpr {
        return namedExpression.copy(alias = namedExpression.alias ?: "_col$index")
    }

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


// Tablename.identifer will be wrongly represented as idx(Tablename, identifier)
// Tablename.identifer.subfield will be idx(idx(Tablename, identifier), subfield)
// This visitor is here to correct these.
private object NormalizeIdentifiersVisitor: LogicalVisitor<Unit>() {

    override fun visit(expression: Expression.Function, operator: LogicalOperator, context: Unit): Expression {
        if (expression.functionName == "idx" && expression.parameters.size == 2) {
            val (arg1, arg2) = expression.parameters

            fun LogicalOperator.aliasesInScope(): List<String> = alias?.let { listOf(it) } ?: children.flatMap { it.aliasesInScope() }

            if (arg1 is Expression.Identifier && arg1.field.fieldName in operator.children.flatMap { it.aliasesInScope() }) {
                // This is one we need to fix, child element should either be a constant or
                // another idx function but this may not be the case if someone is manually using
                // the idx something to do something weird
                val tableAlias = arg1.field.fieldName
                when(arg2) {
                    is Expression.Constant -> {
                        if (arg2.value is String) return Expression.Identifier(Field(tableAlias, arg2.value))
                    }
                    is Expression.Function -> {
                        if(arg2.functionName == "idx" && (arg2.parameters.first() is Expression.Constant)) {
                            val identifierConst = arg2.parameters.first() as Expression.Constant
                            val identifier = Expression.Identifier(Field(tableAlias, identifierConst.value as String))
                            val params = listOf(identifier) + arg2.parameters.drop(1)
                            return Expression.Function("idx", params)
                        }
                    }
                }
            }
        }

        return super.visit(expression, operator, context)
    }
}
