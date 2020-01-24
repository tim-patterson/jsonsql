package jsonsql.physical.operators

import jsonsql.physical.*

class LimitOperator(
        private val limit: Int,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val sourceData = source.data(context)
        return sourceData.take(limit).withClose {
            sourceData.close()
        }
    }

    // For explain output
    override fun toString() = "Limit(${limit})"
}