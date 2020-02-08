package jsonsql.query.normalize

import jsonsql.query.Expression
import jsonsql.query.Field
import jsonsql.query.Query
import jsonsql.query.QueryVisitor

/**
 * Tablename.identifer will be wrongly represented as idx(Tablename, identifier)
 * Tablename.identifer.subfield will be idx(idx(Tablename, identifier), subfield)
 * This visitor attempts to correct this
 */
class NormalizeIdentifiersVisitor: QueryVisitor<List<String>>() {
    companion object {
        fun apply(query: Query): Query =
                NormalizeIdentifiersVisitor().visit(query, listOf())
    }


    override fun visit(node: Query.Select, context: List<String>): Query {
        fun tableAliases(node: Query.SelectSource): List<String?> =
                when(node) {
                    is Query.SelectSource.JustATable -> listOf(node.tableAlias)
                    is Query.SelectSource.Join -> tableAliases(node.source1) + tableAliases(node.source2)
                    is Query.SelectSource.InlineView -> listOf(node.tableAlias)
                    is Query.SelectSource.LateralView -> tableAliases(node.source)
                }
        val aliases = tableAliases(node.source).filterNotNull()

        // Walk everything except for order by's here as order by's can't be fully qualified
        return node.copy(
                expressions = node.expressions.map { visit(it, aliases) },
                groupBy = node.groupBy?.let { it.map { visit(it, aliases) } },
                predicate = node.predicate?.let { visit(it, aliases) },
                // TODO here for the join conditions but should they be able to see the table aliases in joins below them?
                source = visit(node.source, aliases)
        )
    }

    override fun visit(node: Expression.Function, context: List<String>): Expression {
        if (node.functionName == "idx" && node.parameters.size == 2) {
            val (arg1, arg2) = node.parameters

            if (arg1 is Expression.Identifier && arg1.field.fieldName in context) {
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

        return super.visit(node, context)
    }
}