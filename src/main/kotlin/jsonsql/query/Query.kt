package jsonsql.query


/**
 * Data structure that represents a query as we think about it from the sql point of view
 */
sealed class Query {
    data class Explain(val query: Query): Query()
    data class Select(val expressions: List<NamedExpr>, val source: SelectSource, val predicate: Expression?=null, val groupBy: List<Expression>?=null, val orderBy: List<OrderExpr>?=null, val limit: Int?=null) : Query()
    data class Describe(val tbl: Table, val tableOutput: Boolean) : Query()
    data class Insert(val query: Query, val tbl: Table): Query()

    /**
     * The source for a select query
     */
    sealed class SelectSource {
        data class JustATable(val table: Table, val tableAlias: String?): SelectSource()
        data class InlineView(val inner: Query, val tableAlias: String?): SelectSource()
        data class LateralView(val source: SelectSource, val expression: NamedExpr): SelectSource()
        data class Join(val source1: SelectSource, val source2: SelectSource, val joinCondition: Expression): SelectSource()
    }
}

/**
 * Represents any sql expression, scalar or aggregate
 */
sealed class Expression {
    data class Function(val functionName: String, val parameters: List<Expression>) : Expression()
    data class Constant(val value: Any?) : Expression()
    // The table alias here will always be null coming out of the parse function as we can't
    // tell the difference between a table_alias.field vs a field.subfield until we've done
    // some semantic analysis in the logical phase of query planning
    data class Identifier(val field: Field): Expression()
}
// For select and lateral view
data class NamedExpr(val expression: Expression, val alias: String?)
// For order bys and potentially window functions
data class OrderExpr(val expression: Expression, val asc: Boolean)

// Table may not be the right word here...
// Fields will initially be empty but will be populated in our semantic planning phase(normalize)
data class Table(val type: TableType, val path: String, val fields: List<String>)

data class Field(val tableAlias: String?, val fieldName: String) {
    override fun toString() = tableAlias?.let { "$it.$fieldName" } ?: fieldName
}

enum class TableType { CSV, JSON, DIR }
