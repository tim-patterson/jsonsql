package lambdadb.functions


object NumberInspector {
    fun inspect(value: Any?): Double? {
        return when(value) {
            is Double -> value
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