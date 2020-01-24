package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.fileformats.FileFormat
import jsonsql.physical.*

class WriteOperator(
        private val table: Ast.Table,
        private val source: PhysicalOperator
): PhysicalOperator() {

    override val columnAliases = listOf(Field(null, "result"))

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val tableWriter = FileFormat.writer(table, source.columnAliases.map { it.fieldName })
        var rowCount = 0
        source.data(context).use { sourceData ->
            sourceData.forEach { row ->
                tableWriter.write(row)
                rowCount++
            }
        }
        tableWriter.close()
        return sequenceOf(listOf("$rowCount rows written to \"${table.path}\"")).withClose()
    }

    // For explain output
    override fun toString() = "Write(\"${table}\")"
}