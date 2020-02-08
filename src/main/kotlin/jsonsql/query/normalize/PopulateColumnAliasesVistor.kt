package jsonsql.query.normalize

import jsonsql.query.Query
import jsonsql.query.QueryVisitor

/**
 * This just adds column aliases to expressions that don't already have them
 */
class PopulateColumnAliasesVistor: QueryVisitor<Unit>() {
    companion object {
        fun apply(query: Query): Query =
                PopulateColumnAliasesVistor().accept(query, Unit)
    }


    override fun visit(node: Query.Select, context: Unit): Query {
        val fixedExpressions = node.expressions.mapIndexed { index, namedExpr ->
            namedExpr.copy(alias = namedExpr.alias ?: "_col$index")
        }

        return walk(node.copy(expressions = fixedExpressions), context)
    }

    override fun visit(node: Query.SelectSource.LateralView, context: Unit): Query.SelectSource {
        if (node.expression.alias == null) {
            error("Lateral View alias can't be automatically inferred for expression")
        }
        return walk(node, context)
    }

}