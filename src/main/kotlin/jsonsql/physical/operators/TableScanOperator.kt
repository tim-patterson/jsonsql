package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.fileformats.FileFormat
import jsonsql.physical.*

class TableScanOperator(
        private val table: Ast.Table,
        private val fields: List<String>,
        private val tableAlias: String?
): PhysicalOperator() {

    override val columnAliases = fields.map { Field(tableAlias, it) }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        // Enable's support for the gather operator
        val subTable = table.copy(path = context.pathOverrides.getOrDefault(table.path, table.path))
        val tableReader = FileFormat.reader(subTable)
        return generateSequence { tableReader.next() }
                .map { raw ->
                    columnAliases.map { field ->
                        if (field.fieldName == "__all__") {
                            raw
                        } else {
                            raw.get(field.fieldName)
                        }
                    }
                }.withClose {
                    tableReader.close()
                }
    }
    // For explain output
    override fun toString() = "TableScan(\"${table}\" columns=${fields})"
}