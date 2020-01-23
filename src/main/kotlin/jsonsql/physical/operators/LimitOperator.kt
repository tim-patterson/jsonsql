package jsonsql.physical.operators

import jsonsql.physical.ClosableSequence
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.Tuple
import jsonsql.physical.withClose

class LimitOperator(
        private val limit: Int,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(): ClosableSequence<Tuple> {
        val sourceData = source.data()
        return sourceData.take(limit).withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Limit(${limit})"
}