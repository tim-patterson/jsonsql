package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.logical.LogicalTree
import jsonsql.physical.operators.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.VarCharVector

/**
 * Old style operator,
 * TODO remove :)
 */
@Deprecated("Implement VectorizedPhysicalOperator instead ")
abstract class PhysicalOperator: VectorizedPhysicalOperator() {
    abstract override fun next(): List<Any?>?

    /**
     * Glue function during switchover
     */
    override fun data(): Sequence<RecordBatch> {
        // TODO Memory Leak!
        val allocator = RootAllocator()
        return generateSequence(::next).map { rawTuple ->
            val vectors = columnAliases().zip(rawTuple).map { (field, value) ->
                val vector: FieldVector = when(value) {
                    null -> VarCharVector(field.fieldName, allocator).apply {
                        valueCount = 1
                        setNull(0)
                    }
                    is Number -> Float8Vector(field.fieldName, allocator).apply {
                        valueCount = 1
                        setSafe(0, value.toDouble())
                    }
                    else -> VarCharVector(field.fieldName, allocator).apply {
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

abstract class VectorizedPhysicalOperator: AutoCloseable {
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
            nextIter = rowSequence().map { it.values.toList() }.iterator()
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
 */
fun VectorizedPhysicalOperator.rowSequence(): Sequence<Map<String, Any?>>  {
    return data().flatMap { batch ->
        var idx = 0
        val endIdx = batch.recordCount
        generateSequence {
            if (idx < endIdx) {
                val ret = batch.vectorsByName.mapValues { (name, vector) -> vector.getObject(idx) }
                idx++
                ret
            } else {
                null
            }
        }
    }
}




fun physicalOperatorTree(operatorTree: LogicalTree): PhysicalTree {
    val root = physicalOperator(operatorTree.root, operatorTree.streaming)
    root.compile()
    return PhysicalTree(root, operatorTree.streaming && root !is ExplainOperator)
}

private fun physicalOperator(operator: LogicalOperator, streaming: Boolean, pathOverride: String? = null) : VectorizedPhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition, operator.tableOutput)
        is LogicalOperator.DataSource -> TableScanOperator(pathOverride?.let { operator.tableDefinition.copy(path = it) } ?: operator.tableDefinition, operator.fields().map { it.fieldName }, streaming, operator.alias)
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator, streaming, pathOverride), operator.alias)
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.LateralView -> LateralViewOperator(operator.expression, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Join -> JoinOperator(operator.onClause, physicalOperator(operator.sourceOperator1, streaming), physicalOperator(operator.sourceOperator2, streaming))
        is LogicalOperator.GroupBy -> {
            var sourceOperator = physicalOperator(operator.sourceOperator, streaming, pathOverride)
            if (streaming) {
                StreamingGroupByOperator(operator.expressions, operator.groupByExpressions, sourceOperator, operator.linger, operator.alias)
            } else {
                GroupByOperator(operator.expressions, operator.groupByExpressions, sourceOperator, operator.alias)
            }
        }
        is LogicalOperator.Gather -> {
            val tableSource = getTableSource(operator.sourceOperator)
            val files = FileSystem.listDir(tableSource.path)
            val sources = if (files.none()) {
                listOf(physicalOperator(operator.sourceOperator, streaming))
            } else {
                files.map { physicalOperator(operator.sourceOperator, streaming, it["path"] as String) }.toList()
            }

            GatherOperator(sources, streaming)
        }
        is LogicalOperator.Write -> WriteOperator(operator.tableDefinition, physicalOperator(operator.sourceOperator, streaming))
    }
}

data class PhysicalTree(val root: VectorizedPhysicalOperator, val streaming: Boolean)

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children.map { getTableSource(it) }.first()
    }
}
