package jsonsql.functions


object NumberInspector {
    fun inspect(value: Any?): Double? {
        return when(value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
            else -> null
        }
    }
}

object MapInspector {
    @Suppress("UNCHECKED_CAST")
    fun inspect(value: Any?): Map<String,*>? {
        return when(value) {
            is Map<*,*> -> value as Map<String,*>
            else -> null
        }
    }
}

object ArrayInspector {
    fun inspect(value: Any?): List<*>? {
        return when(value) {
            is List<*> -> value
            is Array<*> -> value.asList()
            else -> null
        }
    }
}

object StringInspector {
    fun inspect(value: Any?): String? {
        return when(value) {
            is String -> value
            null -> null
            else -> value.toString()
        }
    }
}

object BooleanInspector {
    fun inspect(value: Any?): Boolean? {
        return when(value) {
            is Boolean -> value
            is String -> when {
                value.equals("true", true) -> true
                value.equals("false", true) -> false
                else -> null
            }
            else -> null
        }
    }
}

fun compareValues(val1: Any?, val2: Any?): Int {
    if (val1 == val2) return 0
    if (val1 == null) return -1
    if (val2 == null) return 1

    if (val1 is Number && val2 is Number) return val1.toDouble().compareTo(val2.toDouble())

    return val1.toString().compareTo(val2.toString())
}