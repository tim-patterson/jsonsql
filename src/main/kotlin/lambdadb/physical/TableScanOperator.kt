package lambdadb.physical

import lambdadb.fileformats.JsonReader

class TableScanOperator(val tableGlob: String, val fields: List<String>): Operator() {
    private val tableReader = JsonReader(tableGlob)

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
    override fun toString() = "TableScan(\"${tableGlob}\" columns=${fields})"
}