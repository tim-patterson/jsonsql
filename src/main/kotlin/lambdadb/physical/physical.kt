package lambdadb.physical

import lambdadb.logical.LogicalOperator

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
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition.glob)
        is LogicalOperator.DataSource -> TableScanOperator(operator.tableDefinition.glob, operator.fields())
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator))
    }
}
