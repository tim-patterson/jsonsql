package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.fileformats.FileFormat
import jsonsql.physical.ClosableSequence
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.Tuple
import jsonsql.physical.withClose

class TableScanOperator(
        private val table: Ast.Table,
        private val fields: List<String>,
        private val streaming: Boolean,
        private val tableAlias: String?
): PhysicalOperator() {

    override val columnAliases = fields.map { Field(tableAlias, it) }

    override fun data(): ClosableSequence<Tuple> {
        val tableReader = FileFormat.reader(table, !streaming)
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