package jsonsql.fileformats

import jsonsql.ast.Ast
import jsonsql.ast.TableType
import jsonsql.ast.TableType.*

interface FileFormat {

    fun reader(path: String): Reader
    fun writer(path: String): Writer

    companion object {
        fun reader(table: Ast.Table) = forType(table.type).reader(table.path)
        fun writer(table: Ast.Table) = forType(table.type).writer(table.path)

        private fun forType(tableType: TableType) =
            when (tableType) {
                CSV -> CsvFormat
                JSON -> JsonFormat
            }
    }

    interface Reader {
        fun next(): Map<String,*>?
        fun close()
    }

    interface Writer {
        fun write(row: Map<String,Any?>)
        fun close()
    }
}