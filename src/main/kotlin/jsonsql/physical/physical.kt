package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.physical.operators.*

abstract class PhysicalOperator: AutoCloseable {
    abstract fun compile()
    abstract fun columnAliases(): List<Field>
    abstract fun next(): List<Any?>?
    // Only used for the explain output
    open fun children(): List<PhysicalOperator> = listOf()
}

fun physicalOperatorTree(operatorTree: LogicalOperator): PhysicalOperator {
    val tree = physicalOperator(operatorTree)
    tree.compile()
    return tree
}

private fun physicalOperator(operator: LogicalOperator, pathOverride: String? = null) : PhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition)
        is LogicalOperator.DataSource -> TableScanOperator(pathOverride?.let { operator.tableDefinition.copy(path = it) } ?: operator.tableDefinition, operator.fields().map { it.fieldName }, operator.alias)
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator, pathOverride), operator.alias)
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.LateralView -> LateralViewOperator(operator.expression, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Join -> JoinOperator(operator.onClause, physicalOperator(operator.sourceOperator1), physicalOperator(operator.sourceOperator2))
        is LogicalOperator.GroupBy -> {
            var sourceOperator = physicalOperator(operator.sourceOperator, pathOverride)
            // The logical group by is performed by a sort by the group by keys followed by the actual group by operator
            // Unless its an empty group by, then the sort isn't needed
            if (operator.groupByExpressions.isNotEmpty()) {
                val sortExpr = operator.groupByExpressions.map { Ast.OrderExpr(it, true) }
                sourceOperator = SortOperator(sortExpr, sourceOperator)
            }
            GroupByOperator(operator.expressions, operator.groupByExpressions, sourceOperator, operator.alias)
        }
        is LogicalOperator.Gather -> {
            val tableSource = getTableSource(operator.sourceOperator)
            val files = FileSystem.listDir(tableSource.path)
            val sources = if (files.isEmpty()) {
                listOf(physicalOperator(operator.sourceOperator))
            } else {
                files.map { physicalOperator(operator.sourceOperator, it) }
            }

            GatherOperator(sources)
        }
        is LogicalOperator.Write -> WriteOperator(operator.tableDefinition, physicalOperator(operator.sourceOperator))
    }
}

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children().map { getTableSource(it) }.first()
    }
}
