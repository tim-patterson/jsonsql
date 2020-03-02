package jsonsql.physical

import jsonsql.query.Query
import jsonsql.query.Field
import jsonsql.physical.operators.*
import jsonsql.query.Table


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
     * As the close is called on the result of this, any real work shouldn't happen at least until the .iterator
     * method of the returned sequence is called, using the lazySeq helper can help with this
     */
    abstract fun data(context: ExecutionContext): ClosableSequence<Tuple>
}

fun physicalOperatorTree(query: Query): PhysicalTree {
    val root = physicalOperator(query)
    return PhysicalTree(root)
}

private fun physicalOperator(query: Query) : PhysicalOperator {
    return when(query) {
        is Query.Describe -> DescribeOperator(query.tbl, query.tableOutput)
        is Query.Select -> fromSelect(query)
        is Query.Explain -> ExplainOperator(physicalOperator(query.query))
        is Query.Insert -> WriteOperator(query.tbl, physicalOperator(query.query))
    }
}

private fun fromSource(source: Query.SelectSource): PhysicalOperator {
    return when(source) {
        is Query.SelectSource.JustATable -> fromTable(source)
        // TODO point this at fromQuery instead once the alias stuff is sorted properly
        is Query.SelectSource.InlineView -> fromSelect(source.inner as Query.Select, source.tableAlias)
        is Query.SelectSource.LateralView -> {
            LateralViewOperator(source.expression, fromSource(source.source))
        }
        is Query.SelectSource.Join -> fromJoin(source)
    }
}

private fun fromTable(source: Query.SelectSource.JustATable): PhysicalOperator {
    return TableScanOperator(source.table, source.table.fields, source.tableAlias)
}

private fun fromSelect(node: Query.Select, tableAlias: String? = null): PhysicalOperator {
    var operator = fromSource(node.source)

    if (node.predicate != null) {
        operator = FilterOperator(node.predicate, operator)
    }

    operator = if (node.groupBy != null ) {
        val groupByKeys = node.groupBy
         GroupByOperator(node.expressions, groupByKeys, operator, tableAlias)
    } else {
        ProjectOperator(node.expressions, operator, tableAlias)
    }

    if (node.orderBy != null) {
        operator = SortOperator(node.orderBy, operator)
    }

    if (node.limit != null) {
        operator = LimitOperator(node.limit, operator)
    }
    return operator
}

private fun fromJoin(node: Query.SelectSource.Join): JoinOperator {
    val sourceOperator1 = fromSource(node.source1)
    val sourceOperator2 = fromSource(node.source2)
    val expr = node.joinCondition
    return JoinOperator(expr, sourceOperator1, sourceOperator2)
}

data class PhysicalTree(private val root: PhysicalOperator) {
    fun execute() = root.data(ExecutionContext())
    val columnAliases by lazy { root.columnAliases }
}
