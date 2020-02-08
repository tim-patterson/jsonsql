package jsonsql.query

/**
 * Class that can be passed into the visit methods that allows us to see whats in scope etc.
 */
data class Scope (
        val tableAliases: Set<String>,
        val fields: Set<Field>,
        val location: Location = Location.UNDEFINED,
        // Where our source is something like a table and we can ask for any column
        val anyFields: Boolean = false
) {
    fun merge(other: Scope): Scope {
        return Scope(
                tableAliases + other.tableAliases,
                fields + other.fields,
                location,
                anyFields || other.anyFields
        )
    }

    enum class Location { PROJECT, WHERE, GROUP_BY, ORDER_BY, LATERAL_VIEW, JOIN_CONDITION, UNDEFINED }
}

fun Query.outerScope(): Scope =
        when(this) {
            is Query.Select -> this.outerScope()
            is Query.Insert -> Scope(setOf(), setOf(Field(null, "results")))
            is Query.Explain -> Scope(setOf(), setOf(Field(null, "plan")))
            is Query.Describe -> {
                val fieldNames = if (tableOutput) setOf("table") else setOf("column_name", "column_type")
                Scope(setOf(), fieldNames.map { Field(null, it) }.toSet())
            }
        }


fun Query.SelectSource.outerScope(): Scope =
        when(this) {
            is Query.SelectSource.JustATable -> this.outerScope()
            is Query.SelectSource.Join -> this.outerScope()
            is Query.SelectSource.InlineView -> this.outerScope()
            is Query.SelectSource.LateralView -> this.outerScope()
        }

fun Query.SelectSource.JustATable.outerScope(): Scope =
        Scope(
                this.tableAlias?.let { setOf(it) } ?: setOf(),
                setOf(),
                anyFields = true
        )

fun Query.SelectSource.Join.outerScope(): Scope =
        this.source1.outerScope().merge(this.source2.outerScope())

fun Query.SelectSource.InlineView.outerScope(): Scope {
    val subQueryScope = this.inner.outerScope()
    return Scope(
            this.tableAlias?.let { setOf(it) } ?: setOf(),
            subQueryScope.fields.map { it.copy(tableAlias=this.tableAlias)}.toSet(),
            anyFields = subQueryScope.anyFields
    )
}

fun Query.SelectSource.LateralView.outerScope(): Scope {
    val sourceScope = this.source.outerScope()
    return if( this.expression.alias == null ){
        sourceScope
    } else {
        sourceScope.copy(fields = sourceScope.fields + setOf(Field(null, this.expression.alias)))
    }
}

fun Query.Select.outerScope() = Scope(
        setOf(),
        this.expressions.map { it.alias }.filterNotNull().map { Field(null, it) }.toSet()
)

fun Query.Select.innerScope() = this.source.outerScope()

fun Query.SelectSource.LateralView.innerScope() = this.source.outerScope()