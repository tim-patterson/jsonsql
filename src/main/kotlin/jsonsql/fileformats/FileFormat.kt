package jsonsql.fileformats

import jsonsql.ast.Ast
import jsonsql.ast.TableType
import jsonsql.ast.TableType.*
import jsonsql.filesystems.FileSystem

interface FileFormat {

    fun reader(fs: FileSystem, path: String): Reader
    fun writer(fs: FileSystem, path: String, fields: List<String>): Writer
    fun split(): Boolean = true

    companion object {
        fun reader(table: Ast.Table) = forType(table.type).reader(FileSystem.from(table.path), table.path)
        fun writer(table: Ast.Table, fields: List<String>) = forType(table.type).writer(FileSystem.from(table.path), table.path, fields)

        fun forType(tableType: TableType) =
            when (tableType) {
                CSV -> CsvFormat
                JSON -> JsonFormat
                DIR -> DirFormat
            }
    }

    interface Reader {
        fun next(): Map<String,*>?
        fun close()
    }

    interface Writer {
        fun write(row: List<Any?>)
        fun close()
    }
}