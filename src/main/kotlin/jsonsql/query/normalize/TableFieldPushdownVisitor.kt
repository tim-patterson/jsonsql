package jsonsql.query.normalize

import jsonsql.query.*

/**
 * All this does is add the list of needed fields into the "Table" nodes.
 * This visitor should be able to be tweaked in the future to allow replacement of "*".
 * We could maintain a list of scopes that we'd push pop to but in this case we'll use
 * the context parameter of this visitor to pass down that state instead.
 * TODO does this really live here... its really part of the physical...
 */
class TableFieldPushdownVisitor: QueryVisitor<MutableSet<Field>>() {
    companion object {
        fun apply(query: Query): Query =
                TableFieldPushdownVisitor().accept(query, mutableSetOf())
    }

    override fun walk(node: Query, context: MutableSet<Field>): Query {
        // Ensure that we walk down to the query source after all the expressions.
        // In this case we simply down the source again - yuk....
        val query = super.walk(node, context)
        return if (query is Query.Select) {
            query.copy(source = accept(query.source, context))
        } else {
            query
        }
    }

    override fun visit(node: Query.Select, context: MutableSet<Field>): Query {
        // New scope
        return super.visit(node, mutableSetOf())
    }

    override fun visit(node: Query.SelectSource.LateralView, context: MutableSet<Field>): Query.SelectSource {
        // TODO we should the lateral view alias from scope here
        return super.visit(node, context)
    }

    override fun visit(node: Query.SelectSource.JustATable, context: MutableSet<Field>): Query.SelectSource {
        // Return a new sub-tree with table fields populated.
        // TODO fix!
        // val fields = context.filter { it.tableAlias ==  node.tableAlias}.map { it.fieldName }
        val fields = context.map { it.fieldName }.toSet().toList()
        return node.copy(table= node.table.copy(fields = fields))
    }

    override fun visit(node: Expression.Identifier, scope: Scope, context: MutableSet<Field>): Expression {
        context.add(node.field)
        return super.visit(node, scope, context)
    }
}