package jsonsql.logical

import jsonsql.query.*

sealed class LogicalOperator(
        vararg val children: LogicalOperator
) {
    open val alias: String? = null // The table alias of this operator ie from (...) as foo, the foo here gets assigned to the inner project or group by.
    abstract val fields: List<Field> // The fields exposed by the operator

    data class Project(val expressions: List<NamedExpr>, val sourceOperator: LogicalOperator, override val alias: String?): LogicalOperator(sourceOperator) {
        override val fields by lazy { expressions.map { Field(alias, it.alias!!) } }
    }

    data class LateralView(val expression: NamedExpr, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields.filterNot { it.fieldName == expression.alias!!} + Field(alias, expression.alias!!) }
    }

    data class Filter(val predicate: Expression, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Sort(val sortExpressions: List<OrderExpr>, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Limit(val limit: Int, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Describe(val tableDefinition: Table, val tableOutput: Boolean): LogicalOperator() {
        override val fields = if(tableOutput) {
            listOf("table")
        } else {
            listOf("column_name", "column_type")
        }.map { Field(alias, it) }
    }

    data class DataSource(val fieldNames: List<String>, val tableDefinition: Table, override val alias: String?): LogicalOperator() {
        override val fields = fieldNames.map { Field(alias, it) }
    }

    data class Explain(val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields = listOf(Field(null, "plan"))
    }

    data class GroupBy(val expressions: List<NamedExpr>, val groupByExpressions: List<Expression>, val sourceOperator: LogicalOperator, override val alias: String?): LogicalOperator(sourceOperator) {
        override val fields by lazy {  expressions.map { Field(alias, it.alias!!) } }
    }

    data class Join(val sourceOperator1: LogicalOperator, val sourceOperator2: LogicalOperator, val onClause: Expression): LogicalOperator(sourceOperator1, sourceOperator2) {
        override val fields  by lazy { sourceOperator1.fields + sourceOperator2.fields }
    }

    data class Gather(val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { sourceOperator.fields }
    }

    data class Write(val tableDefinition: Table, val sourceOperator: LogicalOperator): LogicalOperator(sourceOperator) {
        override val fields by lazy { listOf(Field(null, "result")) }
    }
}

data class LogicalTree(val root: LogicalOperator)