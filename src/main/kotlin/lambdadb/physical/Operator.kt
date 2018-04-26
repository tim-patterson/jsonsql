package lambdadb.physical

import lambdadb.ast.Ast
import lambdadb.functions.aggregatefunctionRegistry
import java.util.*

abstract class Operator: AutoCloseable {
    abstract fun compile()
    abstract fun columnAliases(): List<String>
    abstract fun next(): List<Any?>?
    // Only used for the explain output
    open fun children(): List<Operator> = listOf()
}

fun buildOperatorTree(stmt: Ast.Statement) : Operator {
    return when(stmt) {
        is Ast.Statement.Describe -> DescribeOperator(stmt.tbl.glob)
        is Ast.Statement.Select -> buildForSelect(stmt)
        is Ast.Statement.Explain -> ExplainOperator(buildOperatorTree(stmt.select))
    }
}

fun buildForSelect(select: Ast.Statement.Select): Operator {

    var identifiers = select.expressions.map(Ast.NamedExpr::expression).flatMap(::neededFields)

    var selectExpressions = select.expressions

    var uniqueIdentifiers = mutableSetOf<String>().apply {
        addAll(identifiers)
        if (select.predicate != null) addAll(neededFields(select.predicate))
        if (select.groupBy != null) select.groupBy.forEach { addAll(neededFields(it)) }
    }

    var operator = when(select.source) {
        is Ast.Source.Table -> TableScanOperator(select.source.table.glob, uniqueIdentifiers.toList())
        is Ast.Source.InlineView -> buildForSelect(select.source.inlineView)
    }

    if(select.predicate != null) {
        operator = FilterOperator(operator, select.predicate)
    }

    // with a group by we may have something like
    // sum(a + b) + 4, ie normalFunction(aggregateFunction(normalFunction)))
    // so we need to split our expressions
    if(select.groupBy != null) {
        // step 1 create keys for group by
        val groupByKeys = select.groupBy.map { Ast.NamedExpr(it, "%_groupbykey_" + UUID.nameUUIDFromBytes(it.toString().toByteArray())) }

        // step 2 TODO substitute expressions in select statement that match group by keys

        val groupByExpressions = mutableListOf<Ast.NamedExpr>()
        // groupByExpressions += select.groupBy

        // step 3 Split outer scalar functions from their inner group by functions
        selectExpressions = selectExpressions.map { splitExpression(it, groupByExpressions) }


        operator = SelectOperator(operator, groupByExpressions)
    }

    operator = SelectOperator(operator, selectExpressions)

    if(select.orderBy != null) {
        operator = SortOperator(operator, select.orderBy)
    }

    if(select.limit != null) {
        operator = LimitOperator(select.limit, operator)
    }

    return operator
}

private fun splitExpression(expression: Ast.NamedExpr, aggregates: MutableList<Ast.NamedExpr>): Ast.NamedExpr {
    return expression.copy(expression = splitExpression(expression.expression, aggregates))
}


// Recurses down until we hit a aggregate function at which point we generate a lookup
private fun splitExpression(expression: Ast.Expression, aggregates: MutableList<Ast.NamedExpr>): Ast.Expression {
    return when(expression) {
        is Ast.Expression.Function -> {
            if(aggregatefunctionRegistry.contains(expression.functionName)) {
                // Aggregate function, add to expression for group by and replace with identity expression
                val lookupAlias = "%_groupby_" + UUID.nameUUIDFromBytes(expression.toString().toByteArray())

                aggregates.add(Ast.NamedExpr(expression, lookupAlias))
                Ast.Expression.Identifier(lookupAlias)
            } else {
                val parameters = expression.parameters.map { splitExpression(it, aggregates) }
                expression.copy(parameters = parameters)
            }
        }
        else ->  expression// Just return constants and identifiers as is
    }
}



private fun neededFields(expression: Ast.Expression): List<String> {
    return when (expression) {
        is Ast.Expression.Identifier -> listOf(expression.identifier)
        is Ast.Expression.Constant -> listOf()
        is Ast.Expression.Function -> expression.parameters.flatMap(::neededFields)
    }
}