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
 * Context passed down for query execution, it could be used for passing down
 * runtime config or as a method of collecting runtime stats etc
 */
data class ExecutionContext(
        // Used to override paths in tablesource's for use with the gather operator
        val pathOverrides: Map<String, String> = mapOf()
)

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
     * Called to start pulling data from this operator.
     * As the close is called on the result of this, any real work should happen inside the sequence object after the
     * return of this method, this is to support proper cleanup on query cancellation etc
     */
    abstract fun data(context: ExecutionContext): ClosableSequence<Tuple>
}

fun physicalOperatorTree(operatorTree: LogicalTree): PhysicalTree {
    val root = physicalOperator(operatorTree.root)
    return PhysicalTree(root)
}

private fun physicalOperator(operator: LogicalOperator) : PhysicalOperator {
    return when(operator) {
        is LogicalOperator.Limit -> LimitOperator(operator.limit, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Sort -> SortOperator(operator.sortExpressions, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Describe -> DescribeOperator(operator.tableDefinition, operator.tableOutput)
        is LogicalOperator.DataSource -> TableScanOperator(operator.tableDefinition, operator.fields().map { it.fieldName }, operator.alias)
        is LogicalOperator.Explain -> ExplainOperator(physicalOperator(operator.sourceOperator))
        is LogicalOperator.Project -> ProjectOperator(operator.expressions, physicalOperator(operator.sourceOperator), operator.alias)
        is LogicalOperator.Filter -> FilterOperator(operator.predicate, physicalOperator(operator.sourceOperator))
        is LogicalOperator.LateralView -> LateralViewOperator(operator.expression, physicalOperator(operator.sourceOperator))
        is LogicalOperator.Join -> JoinOperator(operator.onClause, physicalOperator(operator.sourceOperator1), physicalOperator(operator.sourceOperator2))
        is LogicalOperator.GroupBy -> GroupByOperator(operator.expressions, operator.groupByExpressions, physicalOperator(operator.sourceOperator), operator.alias)

        is LogicalOperator.Gather -> {
            val tableSource = getTableSource(operator.sourceOperator)
            GatherOperator(physicalOperator(operator.sourceOperator), tableSource.path)
        }
        is LogicalOperator.Write -> WriteOperator(operator.tableDefinition, physicalOperator(operator.sourceOperator))
    }
}

data class PhysicalTree(private val root: PhysicalOperator) {
    fun execute() = root.data(ExecutionContext())
    val columnAliases by lazy { root.columnAliases }
}

private fun getTableSource(operator: LogicalOperator): Ast.Table {
    return when (operator) {
        is LogicalOperator.Describe -> operator.tableDefinition
        is LogicalOperator.DataSource -> operator.tableDefinition
        else -> operator.children.map { getTableSource(it) }.first()
    }
}
