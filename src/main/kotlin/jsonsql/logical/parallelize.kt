package jsonsql.logical


// Returns true if the subtree is parallel safe
fun parallelize(logicalOperator: LogicalOperator): Boolean {
    return when(logicalOperator) {
        is LogicalOperator.DataSource -> true
        is LogicalOperator.Filter -> parallelize(logicalOperator.sourceOperator)
        is LogicalOperator.LateralView -> parallelize(logicalOperator.sourceOperator)
        is LogicalOperator.Project -> parallelize(logicalOperator.sourceOperator)
        is LogicalOperator.Describe -> false
        is LogicalOperator.Explain -> parallelize(logicalOperator.sourceOperator)

        is LogicalOperator.Sort -> {
            if (parallelize(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.GroupBy -> {
            // If we're smart enough here we should be able to split the executors at the
            // aggregate functions and calculate partial results
            if (parallelize(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.Limit -> {
            if (parallelize(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.Gather -> false
    }
}
