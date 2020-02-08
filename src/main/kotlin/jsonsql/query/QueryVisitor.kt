package jsonsql.query

/**
 * Class to visit all nodes within the query including expressions, builds up copy of tree as
 * it goes, can be used to build modified copy of tree for optimizations etc
 */
abstract class QueryVisitor<C> {
    open fun visit(node: Query, context: C): Query =
            when(node) {
                is Query.Select -> visit(node, context)
                is Query.Describe -> visit(node, context)
                is Query.Explain -> visit(node, context)
                is Query.Insert -> visit(node, context)
            }

    open fun visit(node: Query.Select, context: C): Query =
            node.copy(
                    expressions = node.expressions.map { visit(it, context) },
                    groupBy = node.groupBy?.let { it.map { visit(it, context) } },
                    predicate = node.predicate?.let { visit(it, context) },
                    source = visit(node.source, context),
                    orderBy = node.orderBy?.let { it.map { visit(it, context) } }
            )

    open fun visit(node: Query.Describe, context: C): Query =
            node.copy(
                    tbl = visit(node.tbl, context)
            )

    open fun visit(node: Query.Explain, context: C): Query =
            node.copy(
                    query = visit(node.query, context)
            )

    open fun visit(node: Query.Insert, context: C): Query =
            node.copy(
                    query = visit(node.query, context),
                    tbl = visit(node.tbl, context)
            )

    open fun visit(node: Query.SelectSource, context: C): Query.SelectSource =
            when(node) {
                is Query.SelectSource.JustATable -> visit(node, context)
                is Query.SelectSource.LateralView -> visit(node, context)
                is Query.SelectSource.Join -> visit(node, context)
                is Query.SelectSource.InlineView -> visit(node, context)
            }

    open fun visit(node: Query.SelectSource.JustATable, context: C): Query.SelectSource =
            node.copy(
                    table = visit(node.table, context)
            )

    open fun visit(node: Query.SelectSource.LateralView, context: C): Query.SelectSource =
            node.copy(
                    source = visit(node.source, context),
                    expression = visit(node.expression, context)
            )

    open fun visit(node: Query.SelectSource.Join, context: C): Query.SelectSource =
            node.copy(
                    joinCondition = visit(node.joinCondition, context),
                    source1 = visit(node.source1, context),
                    source2 = visit(node.source2, context)
            )

    open fun visit(node: Query.SelectSource.InlineView, context: C): Query.SelectSource =
            node.copy(
                    inner = visit(node.inner, context)
            )

    open fun visit(node: NamedExpr, context: C): NamedExpr =
            node.copy(
                    expression = visit(node.expression, context)
            )

    open fun visit(node: OrderExpr, context: C): OrderExpr =
            node.copy(
                    expression = visit(node.expression, context)
            )


    open fun visit(node: Expression, context: C): Expression =
            when(node) {
                is Expression.Constant -> visit(node, context)
                is Expression.Identifier -> visit(node, context)
                is Expression.Function -> visit(node, context)
            }

    open fun visit(node: Expression.Constant, context: C): Expression =
            node

    open fun visit(node: Expression.Identifier, context: C): Expression =
            node

    open fun visit(node: Expression.Function, context: C): Expression =
            node.copy(parameters = node.parameters.map { visit(it, context) })

    open fun visit(node: Table, context: C): Table =
            node
}