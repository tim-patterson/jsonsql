package jsonsql.logical


// Returns true if the subtree is parallel safe
fun parallelize(logicalOperator: LogicalOperator): Boolean {
    return when(logicalOperator) {
        is LogicalOperator.Describe -> false
        is LogicalOperator.DataSource -> true

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
        is LogicalOperator.Join -> {
            if (parallelize(logicalOperator.sourceOperator1)) {
                logicalOperator.sourceOperator1 = LogicalOperator.Gather(logicalOperator.sourceOperator1)
            }

            if (parallelize(logicalOperator.sourceOperator2)) {
                logicalOperator.sourceOperator2 = LogicalOperator.Gather(logicalOperator.sourceOperator2)
            }
            false
        }
        is LogicalOperator.Gather -> false

        else -> logicalOperator.children().all { parallelize(it) }
    }
}
