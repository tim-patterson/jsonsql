package jsonsql.logical

import jsonsql.fileformats.FileFormat


fun parallelize(logicalTree: LogicalTree) = ParallelVisitor.visit(logicalTree, ParallelSafe())

// a variable to pass down to be mutated
// at the point that a node that isn't inherently parallel safe but whose
// children are we should insert a gather operator
private data class ParallelSafe(var safe: Boolean = true)

private object ParallelVisitor: LogicalVisitor<ParallelSafe>() {
    override fun visit(tree: LogicalTree, context: ParallelSafe): LogicalTree {
        return tree.copy(root = insertGather(tree.root))
    }

    override fun visit(operator: LogicalOperator.Describe, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator
    }

    override fun visit(operator: LogicalOperator.Explain, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator.copy(sourceOperator = insertGather(operator.sourceOperator))
    }

    override fun visit(operator: LogicalOperator.DataSource, context: ParallelSafe): LogicalOperator {
        context.safe = FileFormat.forType(operator.tableDefinition.type).split()
        return operator
    }

    override fun visit(operator: LogicalOperator.Sort, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator.copy(sourceOperator = insertGather(operator.sourceOperator))
    }

    override fun visit(operator: LogicalOperator.GroupBy, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator.copy(sourceOperator = insertGather(operator.sourceOperator))
    }

    override fun visit(operator: LogicalOperator.Limit, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator.copy(sourceOperator = insertGather(operator.sourceOperator))
    }

    override fun visit(operator: LogicalOperator.Join, context: ParallelSafe): LogicalOperator {
        context.safe = false
        return operator.copy(
                sourceOperator1 = insertGather(operator.sourceOperator1),
                sourceOperator2 = insertGather(operator.sourceOperator2)
        )
    }

    private fun insertGather(child: LogicalOperator): LogicalOperator {
        val childContext = ParallelSafe()
        val childT = super.visit(child, childContext)
        return if (childContext.safe) {
            LogicalOperator.Gather(childT)
        } else {
            childT
        }
    }
}