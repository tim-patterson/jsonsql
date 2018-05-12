package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.ast.TableType
import jsonsql.fileformats.FileFormat
import jsonsql.physical.PhysicalOperator


class DescribeOperator(val table: Ast.Table): PhysicalOperator() {
    private val columns: Iterator<Pair<String,String>> by lazy(::scanTable)

    override fun columnAliases() = listOf("column_name", "column_type").map { Field(null, it) }

    override fun compile() {}

    override fun next(): List<Any?>? {
        if (!columns.hasNext()) return null

        val row = columns.next()
        return arrayListOf(row.first, row.second)
    }

    override fun close() {} // Noop

    private fun scanTable(): Iterator<Pair<String,String>> {
        val cols = mutableMapOf<String, UsedTypes>()

        val tableReader = FileFormat.reader(table, true)

        for (i in 0 until 2000) {
            val json = tableReader.next()
            json ?: break
            json.forEach { (key, value) ->
                val usedTypes = cols.computeIfAbsent(key, { UsedTypes() })
                populateUsedTypes(usedTypes, value)
            }
        }
        tableReader.close()
        val outRows = cols.map { it.key to it.value.toString() }
        return if (table.type == TableType.JSON) {
            outRows.sortedBy { it.first }.iterator()
        } else {
            outRows.iterator()
        }
    }

    private fun populateUsedTypes(usedTypes: UsedTypes, value: Any?) {
        when(value) {
            is Boolean -> usedTypes.couldBeBoolean = true
            is Number -> usedTypes.couldBeNumber = true
            is String -> usedTypes.couldBeString = true
            is Map<*,*> -> {
                usedTypes.couldBeStruct = true
                value.forEach { (k, v) ->
                    val u = usedTypes.structEntries.computeIfAbsent(k as String, { UsedTypes() })
                    populateUsedTypes(u, v)
                }
            }
            is List<*> -> {
                usedTypes.couldBeArray = true
                if (usedTypes.arrayEntries == null) usedTypes.arrayEntries = UsedTypes()
                value.map { populateUsedTypes(usedTypes.arrayEntries!!, it) }
            }
        }
    }


    // Class to keep track of what we see
    private data class UsedTypes(
            var couldBeNumber: Boolean = false,
            var couldBeString: Boolean = false,
            var couldBeBoolean: Boolean = false,
            var couldBeStruct: Boolean = false,
            var couldBeArray: Boolean = false,
            var arrayEntries: UsedTypes? = null,
            val structEntries: MutableMap<String, UsedTypes> = mutableMapOf()
    ) {
        override fun toString(): String {
            val descriptions = mutableListOf<String>()
            if (couldBeNumber) descriptions.add("Number")
            if (couldBeString) descriptions.add("String")
            if (couldBeBoolean) descriptions.add("Boolean")
            if (couldBeArray) descriptions.add("Array<${arrayEntries!!}>")
            if (couldBeStruct) descriptions.add("Struct<$structEntries>")
            return descriptions.joinToString(" OR ")
        }
    }
}