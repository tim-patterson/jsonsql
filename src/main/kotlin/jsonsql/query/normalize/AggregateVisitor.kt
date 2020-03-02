package jsonsql.query.normalize

import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.*

/**
 * For any aggregate queries this just makes sure that groupby clause is non-null
 */
class AggregateVisitor: QueryVisitor<AggregateVisitor.IsAgg>() {
    companion object {
        fun apply(query: Query): Query =
                AggregateVisitor().accept(query, AggregateVisitor.IsAgg())
    }

    override fun visit(node: Query.Select, context: IsAgg): Query {
        val ctx = IsAgg()
        val n = super.visit(node, ctx) as Query.Select
        return if (n.groupBy != null || !ctx.isAgg) {
            n
        } else {
            n.copy(groupBy = listOf())
        }
    }

    override fun visit(node: Expression.Function, scope: Scope, context: IsAgg): Expression {
        val function = functionRegistry[node.functionName]
        if (function is Function.AggregateFunction) {
            context.isAgg = true
        }
        return super.visit(node, scope, context)
    }

    data class IsAgg(var isAgg: Boolean = false)
}