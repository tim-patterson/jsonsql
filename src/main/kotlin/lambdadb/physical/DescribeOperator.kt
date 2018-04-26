package lambdadb.physical

import lambdadb.fileformats.JsonReader


class DescribeOperator(val tableGlob: String): Operator() {
    private val columns: Iterator<Pair<String,String>> by lazy(::scanTable)

    override fun columnAliases() = listOf("column_name", "column_type")

    override fun compile() {}

    override fun next(): List<Any?>? {
        if (!columns.hasNext()) return null

        val row = columns.next()
        return arrayListOf(row.first, row.second)
    }

    override fun close() {} // Noop

    private fun scanTable(): Iterator<Pair<String,String>> {
        val cols = mutableMapOf<String,UsedTypes>()

        val tableReader = JsonReader(tableGlob)

        for (i in 0 until 10000) {
            val json = tableReader.next()
            json ?: break
            json.forEach { (key, value) ->
                val usedTypes = cols.computeIfAbsent(key, { UsedTypes() })
                populateUsedTypes(usedTypes, value)
            }
        }
        tableReader.close()

        return cols.map{ it.key to it.value.toString() }.sortedBy{ it.first }.iterator()
    }

    private fun populateUsedTypes(usedTypes: UsedTypes, value: Any?) {
        when(value) {
            is Double -> usedTypes.couldBeNumber = true
            is Int -> usedTypes.couldBeNumber = true
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
            var couldBeStruct: Boolean = false,
            var couldBeArray: Boolean = false,
            var arrayEntries: UsedTypes? = null,
            val structEntries: MutableMap<String, UsedTypes> = mutableMapOf()
    ) {
        override fun toString(): String {
            val descriptions = mutableListOf<String>()
            if (couldBeNumber) descriptions.add("Number")
            if (couldBeString) descriptions.add("String")
            if (couldBeArray) descriptions.add("Array<${arrayEntries!!.arrayEntries}>")
            if (couldBeStruct) descriptions.add("Struct<$structEntries>")
            return descriptions.joinToString(" OR ")
        }
    }
}