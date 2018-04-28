package jsonsql.physical.operators

import jsonsql.fileformats.JsonReader
import jsonsql.physical.PhysicalOperator

class TableScanOperator(val path: String, val fields: List<String>): PhysicalOperator() {
    private val tableReader = JsonReader(path)

    override fun columnAliases() = fields

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
    override fun toString() = "TableScan(\"${path}\" columns=${fields})"
}