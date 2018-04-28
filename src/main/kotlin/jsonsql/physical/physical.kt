package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.logical.LogicalOperator
import jsonsql.physical.operators.*

abstract class PhysicalOperator: AutoCloseable {
    abstract fun compile()
    abstract fun columnAliases(): List<String>
    abstract fun next(): List<Any?>?
    // Only used for the explain output
    open fun children(): List<PhysicalOperator> = listOf()
}

fun physicalOperatorTree(operatorTree: LogicalOperator) : PhysicalOperator {
    val tree = physicalOperator(operatorTree)
    tree.compile()
    return tree
}

private fun physicalOperator(operator: LogicalOperator) : PhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition.path)
        is LogicalOperator.DataSource -> TableScanOperator(operator.tableDefinition.path, operator.fields())
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator.sourceOperator))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator))
        is LogicalOperator.LateralView -> LateralViewOperator(operator.expression, physicalOperator(operator.sourceOperator))
        is LogicalOperator.GroupBy -> {
            var sourceOperator = physicalOperator(operator.sourceOperator)
            // The logical group by is performed by a sort by the group by keys followed by the actual group by operator
            // Unless its an empty group by, then the sort isn't needed
            if (operator.groupByExpressions.isNotEmpty()) {
                val sortExpr = operator.groupByExpressions.map { Ast.OrderExpr(it, true) }
                sourceOperator = SortOperator(sortExpr, sourceOperator)
            }
            GroupByOperator(operator.expressions, operator.groupByExpressions, sourceOperator)
        }
    }
}
