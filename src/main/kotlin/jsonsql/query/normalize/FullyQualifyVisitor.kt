package jsonsql.query.normalize

import jsonsql.query.*

/**
 * This visit just makes sure everything is fully qualified,
 * It does this by adding qualifiers to.
 * 1. tables,
 * 2. inline views.
 * and any identifiers that reference them.
 * The only identifers that wont be fully qualified after this are those from lateral views
 */
class FullyQualifyVisitor: QueryVisitor<Unit>() {
    private var aliasCount = 1

    companion object {
        fun apply(query: Query): Query =
                FullyQualifyVisitor().accept(query, Unit)
    }

    override fun visit(node: Query.SelectSource.JustATable, context: Unit): Query.SelectSource {
        return if (node.tableAlias == null) {
            super.visit(node.copy(tableAlias = "\$_table_${aliasCount++}"), context)
        } else {
            super.visit(node, context)
        }
    }

    override fun visit(node: Query.SelectSource.InlineView, context: Unit): Query.SelectSource {
        return if (node.tableAlias == null) {
            super.visit(node.copy(tableAlias = "\$_inlineview_${aliasCount++}"), context)
        } else {
            super.visit(node, context)
        }
    }

    override fun visit(node: Expression.Identifier, scope: Scope, context: Unit): Expression {
        return if (node.field.tableAlias == null && node.field !in scope.fields) {
            var field = scope.fields.firstOrNull { it.fieldName == node.field.fieldName }
            if (field == null && scope.anyFields) {
                assert(scope.tableAliases.size == 1) {"Can't find table alias for field $field in scope $scope"}
                field = node.field.copy(tableAlias = scope.tableAliases.first())
            }
            assert(field != null)
            super.visit(node.copy(field=field!!), scope, context)
        } else {
            super.visit(node, scope, context)
        }
    }
}