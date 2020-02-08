package jsonsql.query.normalize

import jsonsql.query.Query
import jsonsql.query.QueryVisitor

/**
 * This just adds column aliases to expressions that don't already have them
 */
class PopulateColumnAliasesVistor: QueryVisitor<Unit>() {
    companion object {
        fun apply(query: Query): Query =
                PopulateColumnAliasesVistor().visit(query, Unit)
    }


    override fun visit(node: Query.Select, context: Unit): Query {
        val expressions = node.expressions.mapIndexed { index, namedExpr ->
            namedExpr.copy(alias = namedExpr.alias ?: "_col$index")
        }

        return super.visit(node.copy(expressions = expressions), context)
    }

    override fun visit(node: Query.SelectSource.LateralView, context: Unit): Query.SelectSource {
        if (node.expression.alias == null) {
            error("Lateral View alias can't for automatically inferred for expression")
        }
        return super.visit(node, context)
    }

}