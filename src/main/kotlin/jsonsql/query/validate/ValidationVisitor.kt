package jsonsql.query.validate


import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.*

/**
 * This is really just checking that identifiers can be found and that we don't have clashes when resolving them
 */
class ValidationVisitor: QueryVisitor<List<String>>() {
    companion object {
        fun apply(query: Query): Query =
                ValidationVisitor().visit(query, listOf())
    }

    private var inProject: Boolean = false

    /**
     * Yuk a bit of a mess, TODO hand down expression location to the expression visit methods
     */
    override fun visit(node: Query.Select, context: List<String>): Query {
        val selectScope = node.innerScope()
        val orderScope = node.outerScope()

        inProject = true
        val expressions = node.expressions.map { visit(it, selectScope, context) }
        inProject = false

        return node.copy(
                expressions = expressions,
                groupBy = node.groupBy?.let { it.map { visit(it, selectScope, context) } },
                predicate = node.predicate?.let { visit(it, selectScope, context) },
                source = visit(node.source, context),
                orderBy = node.orderBy?.let { it.map { visit(it, orderScope, context) } }
        )
    }

    override fun visit(node: Expression.Identifier, scope: Scope, context: List<String>): Expression {
        val field = node.field
        val sourceFields = scope.fields
        if (field.tableAlias != null) {
            semanticAssert(scope.anyFields || field in sourceFields, "$field not found in $sourceFields")
        } else {
            val matchCount = sourceFields.count { it.fieldName == field.fieldName }

            semanticAssert(matchCount > 0 || scope.anyFields, "$field not found in $sourceFields")
            semanticAssert(matchCount <= 1, "$field is ambiguous in  $sourceFields")
        }

        return node
    }

    override fun visit(node: Expression.Function, scope: Scope, context: List<String>): Expression {
        val function = functionRegistry[node.functionName] ?: error("function \"${node.functionName}\" not found")

        if (!inProject) {
            semanticAssert(function is Function.ScalarFunction, "function \"${node.functionName}\" - aggregate functions not allowed here")
        }
        val paramCount = node.parameters.size
        semanticAssert(function.validateParameterCount(paramCount), "function \"${node.functionName}\" not valid for $paramCount parameters")
        return super.visit(node, scope, context)
    }
}


fun semanticAssert(condition: Boolean, msg: String){
    if (!condition) {
        error(msg)
    }
}