package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*
import java.util.concurrent.*

class StreamingGroupByOperator(val expressions: List<Ast.NamedExpr>, val groupByKeys: List<Ast.Expression>, val source: PhysicalOperator, val linger: Double, val tableAlias: String?): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(): ClosableSequence<Tuple> {
        TODO("To be removed")
    }

    // For explain output
    override fun toString() = "StreamingGroupBy(${expressions}, groupby - ${groupByKeys})"
}

