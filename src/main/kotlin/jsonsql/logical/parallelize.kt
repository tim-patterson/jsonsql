package jsonsql.logical


fun parallelize(logicalOperator: LogicalOperator): LogicalOperator {
    // If the whole tree it parallel safe we'll just gather at the root
    return if (gatherWhereNeeded(logicalOperator)) {
        LogicalOperator.Gather(logicalOperator)
    } else {
        logicalOperator
    }
}

// Returns true if the subtree is parallel safe
private fun gatherWhereNeeded(logicalOperator: LogicalOperator): Boolean {
    return when(logicalOperator) {
        is LogicalOperator.Describe -> false
        is LogicalOperator.Explain -> {
            logicalOperator.sourceOperator = if (gatherWhereNeeded(logicalOperator.sourceOperator)) {
                LogicalOperator.Gather(logicalOperator.sourceOperator)
            } else {
                logicalOperator.sourceOperator
            }
            false
        }
        is LogicalOperator.DataSource -> true

        is LogicalOperator.Sort -> {
            if (gatherWhereNeeded(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.GroupBy -> {
            // If we're smart enough here we should be able to split the executors at the
            // aggregate functions and calculate partial results
            if (gatherWhereNeeded(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.Limit -> {
            if (gatherWhereNeeded(logicalOperator.sourceOperator)) {
                logicalOperator.sourceOperator = LogicalOperator.Gather(logicalOperator.sourceOperator)
            }
            false
        }
        is LogicalOperator.Join -> {
            if (gatherWhereNeeded(logicalOperator.sourceOperator1)) {
                logicalOperator.sourceOperator1 = LogicalOperator.Gather(logicalOperator.sourceOperator1)
            }

            if (gatherWhereNeeded(logicalOperator.sourceOperator2)) {
                logicalOperator.sourceOperator2 = LogicalOperator.Gather(logicalOperator.sourceOperator2)
            }
            false
        }
        is LogicalOperator.Gather -> false

        else -> logicalOperator.children().all { gatherWhereNeeded(it) }
    }
}

