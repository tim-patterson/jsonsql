package jsonsql.query

/**
 * Class to visit all nodes within the query including expressions, every walk results in a new tree.
 * Not the typical GOF visitor, none of the double dispatch stuff.
 * Every visit/postVisit method needs to return a node, this allows us to substitute subtree's as we go.
 * visit methods are called before walking the children, postVisits after.
 * If substituting in the visit methods then
 * it is the responsibility of the subclass to call walk if they wish to continue walking down their transformed node.
 */
abstract class QueryVisitor<C> {
    /**
     * In order to support substituting trees before we walk the children then we actually need to walk at the level of
     * of the return type, ie a function expression could be substituted for a constant, this means internally we need
     * to separate the code that visits the children from that of the parents. Hence we have 3 sets of methods in this
     * class.
     * 1. accept - entry points for visiting a node, this calls the visit methods.
     * 2. walk - walk the children but not the node itself, ie call accept for the child nodes.
     * 3. visit - methods that can be overridden by subclasses
     */

    protected fun accept(node: Query, context: C): Query {
        return when (node) {
            is Query.Select -> accept(node, context)
            is Query.Describe -> accept(node, context)
            is Query.Explain -> accept(node, context)
            is Query.Insert -> accept(node, context)
        }
    }

    protected open fun walk(node: Query, context: C): Query {
        return when (node) {
            is Query.Select -> {
                val selectScope = node.innerScope()
                val orderScope = node.outerScope()
                return node.copy(
                        source = accept(node.source, context),
                        predicate = node.predicate?.let { accept(it, selectScope.copy(location = Scope.Location.WHERE), context) },
                        groupBy = node.groupBy?.let { it.map { accept(it, selectScope.copy(location = Scope.Location.GROUP_BY), context) } },
                        expressions = node.expressions.map { accept(it, selectScope.copy(location = Scope.Location.PROJECT), context) },
                        orderBy = node.orderBy?.let { it.map { accept(it, orderScope.copy(location = Scope.Location.ORDER_BY), context) } }
                )
            }
            is Query.Describe -> node.copy(tbl = accept(node.tbl, context))
            is Query.Explain -> node.copy(query = accept(node.query, context))
            is Query.Insert -> node.copy(query = accept(node.query, context), tbl = accept(node.tbl, context))
        }
    }

    protected fun accept(node: Query.Select, context: C): Query = visit(node, context)
    protected open fun visit(node: Query.Select, context: C): Query = walk(node, context)

    protected fun accept(node: Query.Describe, context: C): Query = visit(node, context)
    protected open fun visit(node: Query.Describe, context: C): Query = walk(node, context)

    protected fun accept(node: Query.Explain, context: C): Query = visit(node, context)
    protected open fun visit(node: Query.Explain, context: C): Query = walk(node, context)

    protected fun accept(node: Query.Insert, context: C): Query = visit(node, context)
    protected open fun visit(node: Query.Insert, context: C): Query = walk(node, context)

    protected fun accept(node: Query.SelectSource, context: C): Query.SelectSource =
            when(node) {
                is Query.SelectSource.JustATable -> accept(node, context)
                is Query.SelectSource.LateralView -> accept(node, context)
                is Query.SelectSource.Join -> accept(node, context)
                is Query.SelectSource.InlineView -> accept(node, context)
            }

    protected fun walk(node: Query.SelectSource, context: C): Query.SelectSource =
            when(node) {
                is Query.SelectSource.JustATable -> node.copy(table = accept(node.table, context))
                is Query.SelectSource.LateralView -> {
                    node.copy(
                            source = accept(node.source, context),
                            expression = accept(node.expression, node.innerScope().copy(location = Scope.Location.LATERAL_VIEW), context)
                    )
                }
                is Query.SelectSource.Join -> {
                    node.copy(
                            joinCondition = accept(node.joinCondition, node.outerScope().copy(location = Scope.Location.JOIN_CONDITION), context),
                            source1 = accept(node.source1, context),
                            source2 = accept(node.source2, context)
                    )
                }
                is Query.SelectSource.InlineView -> node.copy(inner = accept(node.inner, context))
            }

    protected fun accept(node: Query.SelectSource.JustATable, context: C): Query.SelectSource = visit(node, context)
    protected open fun visit(node: Query.SelectSource.JustATable, context: C): Query.SelectSource = walk(node, context)

    protected fun accept(node: Query.SelectSource.LateralView, context: C): Query.SelectSource = visit(node, context)
    protected open fun visit(node: Query.SelectSource.LateralView, context: C): Query.SelectSource = walk(node, context)

    protected fun accept(node: Query.SelectSource.Join, context: C): Query.SelectSource = visit(node, context)
    protected open fun visit(node: Query.SelectSource.Join, context: C): Query.SelectSource = walk(node, context)

    protected fun accept(node: Query.SelectSource.InlineView, context: C): Query.SelectSource = visit(node, context)
    protected open fun visit(node: Query.SelectSource.InlineView, context: C): Query.SelectSource = walk(node, context)

    protected fun accept(node: NamedExpr, scope: Scope, context: C): NamedExpr =
            node.copy(
                    expression = accept(node.expression, scope, context)
            )

    protected fun accept(node: OrderExpr, scope: Scope, context: C): OrderExpr =
            node.copy(
                    expression = accept(node.expression, scope, context)
            )

    protected fun accept(node: Expression, scope: Scope, context: C): Expression {
        return when(node) {
            is Expression.Constant -> accept(node, scope, context)
            is Expression.Identifier -> accept(node, scope, context)
            is Expression.Function -> accept(node, scope, context)
        }
    }

    // walk method for all expressions
    protected fun walk(node: Expression, scope: Scope, context: C): Expression =
            when(node) {
                is Expression.Constant -> node // no children
                is Expression.Identifier -> node // no children
                is Expression.Function -> {
                    node.copy(parameters = node.parameters.map { accept(it, scope, context) })
                }
            }

    protected fun accept(node: Expression.Constant, scope: Scope, context: C): Expression = visit(node, scope, context)
    protected open fun visit(node: Expression.Constant, scope: Scope, context: C): Expression = walk(node, scope, context)

    protected fun accept(node: Expression.Identifier, scope: Scope, context: C): Expression = visit(node, scope, context)
    protected open fun visit(node: Expression.Identifier, scope: Scope, context: C): Expression = walk(node, scope, context)

    protected fun accept(node: Expression.Function, scope: Scope, context: C): Expression = visit(node, scope, context)
    protected open fun visit(node: Expression.Function, scope: Scope, context: C): Expression = walk(node, scope, context)

    protected fun accept(node: Table, context: C): Table = node
}