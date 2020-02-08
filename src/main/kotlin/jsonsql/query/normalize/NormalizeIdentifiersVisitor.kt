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
                NormalizeIdentifiersVisitor().visit(query, Unit)
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
                    is Expression.Function -> {
                        if(arg2.functionName == "idx" && (arg2.parameters.first() is Expression.Constant)) {
                            val identifierConst = arg2.parameters.first() as Expression.Constant
                            val identifier = Expression.Identifier(Field(tableAlias, identifierConst.value as String))
                            val params = listOf(identifier) + arg2.parameters.drop(1)
                            return Expression.Function("idx", params)
                        }
                    }
                }
            }
        }

        return super.visit(node, scope, context)
    }
}