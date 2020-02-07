package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*
import java.lang.Exception

class GroupByOperator(
        private val expressions: List<Ast.NamedExpr>,
        private val groupByKeys: List<Ast.Expression>,
        private val source: PhysicalOperator,
        private val tableAlias: String?
): PhysicalOperator() {

    override val columnAliases = expressions.map { Field(tableAlias, it.alias!!) }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val compiledGroupExpressions = compileExpressions(groupByKeys, source.columnAliases)

        val sourceData = source.data(context)

        // Due to the .aggregate not being lazy we need to wrap this whole thing in a try catch.
        try {
            var aggregates = sourceData.groupingBy { row -> compiledGroupExpressions.map { it.evaluate(row) } }
                    .aggregate { _, accumulator: List<AggregateExpressionExecutor>?, element, _ ->
                        val exprs = accumulator
                                ?: compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases)
                        exprs.map { it.processRow(element) }

                        exprs
                    }
            // Special case for select count(*) from ... where 1=2
            // Ie we still want to return 0, not no rows.
            if (groupByKeys.isEmpty() && aggregates.isEmpty()) {
                aggregates = mapOf(listOf<Any?>() to compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases))
            }

            return aggregates.asSequence().map { (_, valuesExprs) ->
                valuesExprs.map { it.getResult() }
            }.withClose {
                sourceData.close()
            }
        } catch (e: Exception) {
            sourceData.close()
            throw e
        }
    }

    // For explain output
    override fun toString() = "GroupBy(${expressions}, groupby - ${groupByKeys})"
}

