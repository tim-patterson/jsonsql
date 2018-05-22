package jsonsql.logical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry

sealed class LogicalOperator(vararg val children: LogicalOperator) {
    abstract var alias: String?
    abstract fun fields(): List<Field>

    // this is really aliases in scope for anything that uses this operator as a source
    open fun aliasesInScope(): List<String> = alias?.let { listOf(it) } ?: children.flatMap { it.aliasesInScope() }

    data class Project(val expressions: List<Ast.NamedExpr>, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = expressions.map { Field(alias, it.alias!!) }
    }

    data class LateralView(val expression: Ast.NamedExpr, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = sourceOperator.fields().filterNot { it.fieldName == expression.alias!!} + Field(alias, expression.alias!!)
    }

    data class Filter(val predicate: Ast.Expression, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = sourceOperator.fields()
    }

    data class Sort(val sortExpressions: List<Ast.OrderExpr>, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = sourceOperator.fields()
    }

    data class Limit(val limit: Int, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = sourceOperator.fields()
    }

    data class Describe(val tableDefinition: Ast.Table, override var alias: String? = null): LogicalOperator() {
        override fun fields() = listOf("column_name", "column_type").map { Field(alias, it) }
    }

    data class DataSource(val fields: List<String>, val tableDefinition: Ast.Table, override var alias: String? = null): LogicalOperator() {
        override fun fields() = fields.map { Field(alias, it) }
    }

    data class Explain(val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = listOf(Field(null, "plan"))
    }

    data class GroupBy(val expressions: List<Ast.NamedExpr>, val groupByExpressions: List<Ast.Expression>, val linger: Double, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = expressions.map { Field(alias, it.alias!!) }
    }

    data class Join(val sourceOperator1: LogicalOperator, val sourceOperator2: LogicalOperator, val onClause: Ast.Expression, override var alias: String? = null): LogicalOperator(sourceOperator1, sourceOperator2) {
        override fun fields() = sourceOperator1.fields() + sourceOperator2.fields()
    }

    data class Gather(val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = sourceOperator.fields()
    }

    data class Write(val tableDefinition: Ast.Table, val sourceOperator: LogicalOperator, override var alias: String? = null): LogicalOperator(sourceOperator) {
        override fun fields() = listOf(Field(null, "Result"))
    }
}

data class LogicalTree(val root: LogicalOperator, val streaming: Boolean)


fun logicalOperatorTree(stmt: Ast.Statement) : LogicalTree {
    var tree = when(stmt) {
        is Ast.Statement.Describe -> LogicalTree(LogicalOperator.Describe(stmt.tbl), false)
        is Ast.Statement.Select -> LogicalTree(fromSelect(stmt), stmt.streaming)
        is Ast.Statement.Explain -> LogicalTree(LogicalOperator.Explain(fromSelect(stmt.select)), stmt.select.streaming)
        is Ast.Statement.Insert -> LogicalTree(LogicalOperator.Write(stmt.tbl, fromSelect(stmt.select)), stmt.select.streaming)
    }

    tree = NormalizeIdentifiersVisitor.visit(tree, Unit)
    tree = PopulateFieldsVisitor.visit(tree, setOf())
    tree = validate(tree)
    tree = ExpressionOptimizer.visit(tree, Unit)
    return parallelize(tree)
}

private fun fromSource(node: Ast.Source): LogicalOperator {
    return when(node) {
        is Ast.Source.Table -> fromTable(node.table).apply { alias = node.tableAlias }
        is Ast.Source.InlineView -> fromSelect(node.inlineView).apply { alias = node.tableAlias }
        is Ast.Source.LateralView -> {
            val source = fromSource(node.source)
            val expr = (node.expression.copy(expression = node.expression.expression))
            LogicalOperator.LateralView(expr, source)
        }
        is Ast.Source.Join -> fromJoin(node)
    }
}


private fun fromJoin(node: Ast.Source.Join): LogicalOperator.Join {
    val sourceOperator1 = fromSource(node.source1)
    val sourceOperator2 = fromSource(node.source2)
    val expr = node.joinCondition
    return LogicalOperator.Join(sourceOperator1, sourceOperator2, expr)
}


private fun fromSelect(node: Ast.Statement.Select): LogicalOperator {
    var operator = fromSource(node.source)

    if (node.predicate != null) {
        operator = LogicalOperator.Filter(node.predicate, operator)
    }

    // if we've got aggregation functions in the select but no group by, its really still a group by
    if (node.groupBy != null || node.expressions.map { checkForAggregate(it.expression) }.any { it } ) {
        val groupByKeys = node.groupBy ?: listOf()

        operator = LogicalOperator.GroupBy(node.expressions, groupByKeys, node.linger, operator)
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


private object PopulateFieldsVisitor: LogicalVisitor<Set<Field>>() {
    override fun visit(namedExpression: Ast.NamedExpr, index: Int, operator: LogicalOperator, context: Set<Field>): Ast.NamedExpr {
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
        return operator.copy(fields = fields)
    }

    private fun neededFields(expression: Ast.Expression): Set<Field> {
        return when (expression) {
            is Ast.Expression.Identifier -> setOf(expression.field)
            is Ast.Expression.Constant -> setOf()
            is Ast.Expression.Function -> expression.parameters.flatMap(::neededFields).toSet()
        }
    }
}


// Tablename.identifer will be wrongly represented as idx(Tablename, identifier)
// Tablename.identifer.subfield will be idx(Tablename, idx(identifier, subfield))
// This visitor is here to correct these.
private object NormalizeIdentifiersVisitor: LogicalVisitor<Unit>() {

    override fun visit(expression: Ast.Expression.Function, operator: LogicalOperator, context: Unit): Ast.Expression {
        if (expression.functionName == "idx" && expression.parameters.size == 2) {
            val arg1 = expression.parameters[0]
            val arg2 = expression.parameters[1]
            if (arg1 is Ast.Expression.Identifier && arg1.field.fieldName in operator.children.flatMap { it.aliasesInScope() }) {
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

        return super.visit(expression, operator, context)
    }
}
