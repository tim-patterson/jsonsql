package jsonsql.fileformats

import jsonsql.ast.Ast
import jsonsql.ast.TableType.*

interface FileFormat {
    fun next(): Map<String,*>?
    fun close()

    companion object {
        fun from(table: Ast.Table): FileFormat {
            return when (table.type) {
                CSV -> CsvFormat(table.path)
                JSON -> JsonFormat(table.path)
            }
        }
    }
}