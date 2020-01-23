package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.logical.LogicalTree
import jsonsql.physical.operators.*

/**
 * Interface used for streaming data out of operators, allows something like a limit to signal that its done with the
 * sequence.
 */
interface ClosableSequence<out T>: Sequence<T>, AutoCloseable

/**
 * Wraps a sequence to turn it into a ClosableSequence, we also autoclose at the end of iteration, no attempt is made to
 * prevent multiple close calls
 */
fun <T> Sequence<T>.withClose(onClose: ()->Unit = {}): ClosableSequence<T> {
    return object: ClosableSequence<T> {
        override fun iterator(): Iterator<T> = object: Iterator<T> {
            private val iter = this@withClose.iterator()
            override fun hasNext() = iter.hasNext().also { if (!it) onClose() }
            override fun next(): T {
                try {
                    return iter.next()
                } catch (e: NoSuchElementException) {
                    onClose()
                    throw e
                }
            }
        }
        override fun close() { onClose() }
    }
}

// Represents a row of data flowing through.
typealias Tuple = List<Any?>

/**
 * Main operator class. The lifecycle of this class is
 * 1. creation
 * 2. execution - Ie calling the data method.
 * 3. cleanup - Cleaning up any resources left, this isn't done at the operator level but at the execution level
 *
 * Note the execution method may be called multiple times in parallel from different threads on the same operator
 * instance
 */
abstract class PhysicalOperator {
    // Should expose the column aliases that this operator exposes, used by downstream operators to compile their
    // expressions
    abstract val columnAliases: List<Field>

    /**
     * Called to start pulling data from this operator, should be called once done
     */
    abstract fun data(): ClosableSequence<Tuple>
}

fun physicalOperatorTree(operatorTree: LogicalTree): PhysicalTree {
    val root = physicalOperator(operatorTree.root, operatorTree.streaming)
    return PhysicalTree(root, operatorTree.streaming && root !is ExplainOperator)
}

private fun physicalOperator(operator: LogicalOperator, streaming: Boolean, pathOverride: String? = null) : PhysicalOperator {
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

data class PhysicalTree(val root: PhysicalOperator, val streaming: Boolean)

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children.map { getTableSource(it) }.first()
    }
}
