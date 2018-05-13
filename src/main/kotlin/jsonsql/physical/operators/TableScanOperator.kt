package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.fileformats.FileFormat
import jsonsql.physical.PhysicalOperator

class TableScanOperator(val table: Ast.Table, val fields: List<String>, val streaming: Boolean, val tableAlias: String?): PhysicalOperator() {
    private val tableReader = FileFormat.reader(table, !streaming)

    override fun columnAliases() = fields.map { Field(tableAlias, it) }

    override fun compile() {}

    override fun next(): List<Any?>? {
        val raw = tableReader.next()
        raw ?: return null

        return fields.map {
            if (it == "__all__") {
                raw
            } else {
                raw.get(it)
            }
        }
    }

    override fun close() = tableReader.close()

    // For explain output
    override fun toString() = "TableScan(\"${table}\" columns=${fields})"
}