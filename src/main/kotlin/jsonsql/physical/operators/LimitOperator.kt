package jsonsql.physical.operators

import jsonsql.physical.PhysicalOperator
import jsonsql.physical.VectorizedPhysicalOperator

class LimitOperator(val limit: Int, val source: VectorizedPhysicalOperator): PhysicalOperator() {
    private var offset = 0

    override fun columnAliases() = source.columnAliases()

    override fun compile() = source.compile()

    override fun next(): List<Any?>? {
        if (offset >= limit) {
            source.close()
            return null
        }
        offset++
        return source.next()
    }

    override fun close() = source.close()

    override fun toString() = "Limit(${limit})"
}