package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.fileformats.FileFormat
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.VectorizedPhysicalOperator

class WriteOperator(val table: Ast.Table, val source: VectorizedPhysicalOperator): PhysicalOperator() {
    private val tableWriter = FileFormat.writer(table, source.columnAliases().map { it.fieldName })
    private var isDone = false

    override fun columnAliases() = listOf(Field(null, "Result"))

    override fun compile() {
        source.compile()
    }

    override fun next(): List<Any?>? {
        if (isDone) return null

        isDone = true
        var rowCount = 0
        while (true) {
            val row = source.next()
            row ?: return listOf("$rowCount rows written to \"${table.path}\"")
            tableWriter.write(row)
            rowCount++
        }
    }

    override fun close() = tableWriter.close()

    override fun toString() = "Write(\"${table}\")"
}