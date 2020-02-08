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

    open fun visit(node: Query.Select, context: C): Query {
        val selectScope = node.innerScope()
        val orderScope = node.outerScope()
        return node.copy(
                expressions = node.expressions.map { visit(it, selectScope, context) },
                groupBy = node.groupBy?.let { it.map { visit(it, selectScope, context) } },
                predicate = node.predicate?.let { visit(it, selectScope, context) },
                source = visit(node.source, context),
                orderBy = node.orderBy?.let { it.map { visit(it, orderScope, context) } }
        )
    }

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
                    expression = visit(node.expression, node.innerScope(), context)
            )

    open fun visit(node: Query.SelectSource.Join, context: C): Query.SelectSource =
            node.copy(
                    joinCondition = visit(node.joinCondition, node.outerScope(), context),
                    source1 = visit(node.source1, context),
                    source2 = visit(node.source2, context)
            )

    open fun visit(node: Query.SelectSource.InlineView, context: C): Query.SelectSource =
            node.copy(
                    inner = visit(node.inner, context)
            )

    open fun visit(node: NamedExpr, scope: Scope, context: C): NamedExpr =
            node.copy(
                    expression = visit(node.expression, scope, context)
            )

    open fun visit(node: OrderExpr, scope: Scope, context: C): OrderExpr =
            node.copy(
                    expression = visit(node.expression, scope, context)
            )


    open fun visit(node: Expression, scope: Scope, context: C): Expression =
            when(node) {
                is Expression.Constant -> visit(node, scope, context)
                is Expression.Identifier -> visit(node, scope, context)
                is Expression.Function -> visit(node, scope, context)
            }

    open fun visit(node: Expression.Constant, scope: Scope, context: C): Expression =
            node

    open fun visit(node: Expression.Identifier, scope: Scope, context: C): Expression =
            node

    open fun visit(node: Expression.Function, scope: Scope, context: C): Expression =
            node.copy(parameters = node.parameters.map { visit(it, scope, context) })

    open fun visit(node: Table, context: C): Table =
            node
}

/**
 * Class that can be passed into the visit methods that allows us to see whats in scope etc.
 */
data class Scope (
        val tableAliases: Set<String>,
        val fields: Set<Field>,
        // Where our source is something like a table and we can ask for any column
        val anyFields: Boolean = false
) {
    fun merge(other: Scope): Scope {
        return Scope(
                tableAliases + other.tableAliases,
                fields + other.fields,
                anyFields || other.anyFields
        )
    }
}

fun Query.outerScope(): Scope =
        when(this) {
            is Query.Select -> this.outerScope()
            is Query.Insert -> Scope(setOf(), setOf(Field(null, "results")))
            is Query.Explain -> Scope(setOf(), setOf(Field(null, "plan")))
            is Query.Describe -> {
                val fieldNames = if (tableOutput) setOf("table") else setOf("column_name", "column_type")
                Scope(setOf(), fieldNames.map { Field(null, it) }.toSet())
            }
        }


fun Query.SelectSource.outerScope(): Scope =
        when(this) {
            is Query.SelectSource.JustATable -> this.outerScope()
            is Query.SelectSource.Join -> this.outerScope()
            is Query.SelectSource.InlineView -> this.outerScope()
            is Query.SelectSource.LateralView -> this.outerScope()
        }

fun Query.SelectSource.JustATable.outerScope(): Scope =
        Scope(
                this.tableAlias?.let { setOf(it) } ?: setOf(),
                setOf(),
                anyFields = true
        )

fun Query.SelectSource.Join.outerScope(): Scope =
        this.source1.outerScope().merge(this.source2.outerScope())

fun Query.SelectSource.InlineView.outerScope(): Scope {
        val subQueryScope = this.inner.outerScope()
        return Scope(
                this.tableAlias?.let { setOf(it) } ?: setOf(),
                subQueryScope.fields.map { it.copy(tableAlias=this.tableAlias)}.toSet(),
                subQueryScope.anyFields
        )
}

fun Query.SelectSource.LateralView.outerScope(): Scope {
    val sourceScope = this.source.outerScope()
    return if( this.expression.alias == null ){
        sourceScope
    } else {
        sourceScope.copy(fields = sourceScope.fields + setOf(Field(null, this.expression.alias)))
    }
}

fun Query.Select.outerScope() = Scope(
        setOf(),
        this.expressions.map { it.alias }.filterNotNull().map { Field(null, it) }.toSet()
)

fun Query.Select.innerScope() = this.source.outerScope()

fun Query.SelectSource.LateralView.innerScope() = this.source.outerScope()