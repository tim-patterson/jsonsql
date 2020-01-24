package jsonsql.physical

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.filesystems.FileSystem
import jsonsql.logical.LogicalOperator
import jsonsql.logical.LogicalTree
import jsonsql.physical.operators.*


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
    val root = physicalOperator(operatorTree.root)
    return PhysicalTree(root)
}

private fun physicalOperator(operator: LogicalOperator, pathOverride: String? = null) : PhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition, operator.tableOutput)
        is LogicalOperator.DataSource -> TableScanOperator(pathOverride?.let { operator.tableDefinition.copy(path = it) } ?: operator.tableDefinition, operator.fields().map { it.fieldName }, operator.alias)
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator, pathOverride), operator.alias)
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.LateralView -> LateralViewOperator(operator.expression, physicalOperator(operator.sourceOperator, pathOverride))
        is LogicalOperator.Join -> JoinOperator(operator.onClause, physicalOperator(operator.sourceOperator1), physicalOperator(operator.sourceOperator2))
        is LogicalOperator.GroupBy -> {
            var sourceOperator = physicalOperator(operator.sourceOperator, pathOverride)
            GroupByOperator(operator.expressions, operator.groupByExpressions, sourceOperator, operator.alias)
        }
        is LogicalOperator.Gather -> {
            val tableSource = getTableSource(operator.sourceOperator)
            val files = FileSystem.listDir(tableSource.path)
            val sources = if (files.none()) {
                listOf(physicalOperator(operator.sourceOperator))
            } else {
                files.map { physicalOperator(operator.sourceOperator, it["path"] as String) }.toList()
            }

            GatherOperator(sources, false)
        }
        is LogicalOperator.Write -> WriteOperator(operator.tableDefinition, physicalOperator(operator.sourceOperator))
    }
}

data class PhysicalTree(val root: PhysicalOperator)

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children.map { getTableSource(it) }.first()
    }
}
