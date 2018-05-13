package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.logical.LogicalTree
import jsonsql.physical.operators.*

abstract class PhysicalOperator: AutoCloseable {
    abstract fun compile()
    abstract fun columnAliases(): List<Field>
    abstract fun next(): List<Any?>?
    // Only used for the explain output
    open fun children(): List<PhysicalOperator> = listOf()
}

fun physicalOperatorTree(operatorTree: LogicalTree): PhysicalTree {
    val root = physicalOperator(operatorTree.root, operatorTree.streaming)
    root.compile()
    return PhysicalTree(root, operatorTree.streaming && root !is ExplainOperator)
}

private fun physicalOperator(operator: LogicalOperator, streaming: Boolean, pathOverride: String? = null) : PhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator, streaming, pathOverride))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition)
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
            val sources = if (files.isEmpty()) {
                listOf(physicalOperator(operator.sourceOperator, streaming))
            } else {
                files.map { physicalOperator(operator.sourceOperator, streaming, it["path"] as String) }
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
        else -> operator.children().map { getTableSource(it) }.first()
    }
}
