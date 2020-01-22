package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*

class GroupByOperator(val expressions: List<Ast.NamedExpr>, val groupByKeys: List<Ast.Expression>, val source: VectorizedPhysicalOperator, val tableAlias: String?): PhysicalOperator() {
    private lateinit var compiledGroupExpressions: List<ExpressionExecutor>

    override fun columnAliases() = expressions.map { Field(tableAlias, it.alias!!) }

    private var rowIter: Iterator<List<Any?>>? = null

    override fun compile() {
        source.compile()
        compiledGroupExpressions = compileExpressions(groupByKeys, source.columnAliases())
    }

    // We iterate until we hit the starting key(s) for the next group
    // However as we've already consumed it we have to store it in
    // an ivar for the next group
    override fun next(): List<Any?>? {
        if(rowIter != null) {
            if (rowIter!!.hasNext()){
                return rowIter!!.next()
            } else {
                return null
            }
        }

        val aggs = mutableMapOf<String, List<AggregateExpressionExecutor>>()
        while (true) {

            val sourceRow = source.next()
            if (sourceRow != null) {
                val groupKeys = compiledGroupExpressions.map { it.evaluate(sourceRow) }
                val groupStr = groupKeys.toString()

                val exprs = aggs.computeIfAbsent(groupStr, {
                    compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases())
                })
                exprs.map { it.processRow(sourceRow) }
            } else {
                // In the case of no group by keys, ie select count() from foo
                // we expect to get a count of 0 for zero rows, its not really
                // consistent but that's sql for you
                if (groupByKeys.isEmpty() && aggs.isEmpty()) {
                    aggs[""] = compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases())
                }
                rowIter = aggs.values.map { it.map { it.getResult() } }.iterator()

                if (rowIter!!.hasNext()){
                    return rowIter!!.next()
                } else {
                    return null
                }
            }
        }
    }

    override fun close() {
        source.close()
    }

    override fun toString() = "GroupBy(${expressions}, groupby - ${groupByKeys})"
}

