package jsonsql.query.normalize

import jsonsql.query.*

/**
 * Tablename.identifer will be wrongly represented as idx(Tablename, identifier)
 * Tablename.identifer.subfield will be idx(idx(Tablename, identifier), subfield)
 * This visitor attempts to correct this
 */
class NormalizeIdentifiersVisitor: QueryVisitor<Unit>() {
    companion object {
        fun apply(query: Query): Query =
                NormalizeIdentifiersVisitor().accept(query, Unit)
    }

    override fun visit(node: Expression.Function, scope: Scope, context: Unit): Expression {
        if (scope.tableAliases.isNotEmpty() && node.functionName == "idx" && node.parameters.size == 2) {
            val (arg1, arg2) = node.parameters

            if (arg1 is Expression.Identifier && arg1.field.fieldName in scope.tableAliases) {
                // This is one we need to fix, child element should either be a constant or
                // another idx function but this may not be the case if someone is manually using
                // the idx something to do something weird
                val tableAlias = arg1.field.fieldName
                when(arg2) {
                    is Expression.Constant -> {
                        if (arg2.value is String) return Expression.Identifier(Field(tableAlias, arg2.value))
                    }
                }
            }
        }

        // carry on walking into the expression
        return walk(node, scope, context)
    }
}