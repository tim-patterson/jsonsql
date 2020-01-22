package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.logical.LogicalTree
import jsonsql.physical.operators.*
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VarCharVector

/**
 * Old style operator,
 * TODO remove :)
 */
@Deprecated("Implement VectorizedPhysicalOperator instead ")
abstract class PhysicalOperator(allocator: BufferAllocator):
        VectorizedPhysicalOperator(allocator) {
    abstract override fun next(): List<Any?>?

    /**
     * Glue function during switchover
     */
    override fun data(): Sequence<RecordBatch> {
        return generateSequence(::next).map { rawTuple ->
            val vectors = columnAliases().zip(rawTuple).map { (field, value) ->
                val vector: FieldVector = when(value) {
                    null -> VarCharVector(field.toString(), allocator).apply {
                        valueCount = 1
                        setNull(0)
                    }
                    is Number -> Float8Vector(field.toString(), allocator).apply {
                        valueCount = 1
                        setSafe(0, value.toDouble())
                    }
                    else -> VarCharVector(field.toString(), allocator).apply {
                        valueCount = 1
                        setSafe(0, value.toString().toByteArray())
                    }
                }
                vector
            }
            RecordBatch(vectors)
        }
    }
}

abstract class VectorizedPhysicalOperator(
        protected val allocator: BufferAllocator
): AutoCloseable {
    // TODO we should be able to remove both these once we make the switch fully away from the old style operators
    abstract fun compile()
    abstract fun columnAliases(): List<Field>

    abstract fun data(): Sequence<RecordBatch>

    /**
     * Glue function during switchover
     * TODO remove
     */
    private lateinit var nextIter: Iterator<List<Any?>>
    @Deprecated("use nextBatch instead")
    open fun next(): List<Any?>? {
        if(!::nextIter.isInitialized) {
            nextIter = rowSequence().iterator()
        }
        return if (nextIter.hasNext()) {
            nextIter.next()
        } else {
            null
        }
    }
}


/**
 * Should only really be used for display/debugging purposes etc,
 * TODO should we
 */
fun VectorizedPhysicalOperator.rowSequence(): Sequence<List<Any?>>  {
    return data().flatMap { batch ->
        var idx = 0
        val endIdx = batch.recordCount
        generateSequence {
            if (idx < endIdx) {
                val ret = batch.vectors.map { it.getObject(idx) }
                idx++
                ret
            } else {
                batch.releaseAll()
                null
            }
        }
    }
}




fun physicalOperatorTree(allocator: BufferAllocator, operatorTree: LogicalTree): PhysicalTree {
    val root = physicalOperator(allocator, operatorTree.root, operatorTree.streaming)
    root.compile()
    return PhysicalTree(allocator, root, operatorTree.streaming && root !is ExplainOperator)
}

private fun physicalOperator(allocator: BufferAllocator, operator: LogicalOperator, streaming: Boolean, pathOverride: String? = null) : VectorizedPhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(allocator, operator.limit, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Sort -> SortOperator(allocator, operator.sortExpressions, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Describe -> DescribeOperator(allocator, operator.tableDefinition, operator.tableOutput)
        is LogicalOperator.DataSource -> TableScanOperator(allocator, pathOverride?.let { operator.tableDefinition.copy(path = it) } ?: operator.tableDefinition, operator.fields().map { it.fieldName }, streaming, operator.alias)
        is LogicalOperator.Explain -> ExplainOperator(allocator, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Project -> ProjectOperator(allocator, operator.expressions, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride), operator.alias)
        is LogicalOperator.Filter -> FilterOperator(allocator, operator.predicate, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.LateralView -> LateralViewOperator(allocator, operator.expression, physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Join -> JoinOperator(allocator, operator.onClause, physicalOperator(allocator, operator.sourceOperator1, streaming), physicalOperator(allocator, operator.sourceOperator2, streaming))
        is LogicalOperator.GroupBy -> {
            var sourceOperator = physicalOperator(allocator, operator.sourceOperator, streaming, pathOverride)
            if (streaming) {
                StreamingGroupByOperator(allocator, operator.expressions, operator.groupByExpressions, sourceOperator, operator.linger, operator.alias)
            } else {
                GroupByOperator(allocator, operator.expressions, operator.groupByExpressions, sourceOperator, operator.alias)
            }
        }
        is LogicalOperator.Gather -> {
            val tableSource = getTableSource(operator.sourceOperator)
            val files = FileSystem.listDir(tableSource.path)
            val sources = if (files.none()) {
                listOf(physicalOperator(allocator, operator.sourceOperator, streaming))
            } else {
                files.map { physicalOperator(allocator, operator.sourceOperator, streaming, it["path"] as String) }.toList()
            }

            GatherOperator(allocator, sources, streaming)
        }
        is LogicalOperator.Write -> WriteOperator(allocator, operator.tableDefinition, physicalOperator(allocator, operator.sourceOperator, streaming))
    }
}

data class PhysicalTree(private val allocator: BufferAllocator, val root: VectorizedPhysicalOperator, val streaming: Boolean): AutoCloseable {
    override fun close() {
        root.close()
        allocator.close()
    }
}

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children.map { getTableSource(it) }.first()
    }
}
