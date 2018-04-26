package lambdadb.functions


abstract class ScalarFunction {
    abstract fun execute(args: List<Any?>): Any?
    abstract fun validateParameterCount(count: Int) : Boolean
}

abstract class TwoArgScalarFunction: ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0], args[1])
    }

    override fun validateParameterCount(count: Int) = count == 2

    abstract fun execute(arg1: Any?, arg2: Any?): Any?
}

abstract class OneArgScalarFunction: ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0])
    }

    override fun validateParameterCount(count: Int) = count == 1

    abstract fun execute(arg1: Any?): Any?
}

// gets components out of nested objects
object IndexFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val obj = MapInspector.inspect(arg1)
        obj ?: return null
        val field = StringInspector.inspect(arg2)

        return obj[field]
    }
}

object AddFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val num1 = NumberInspector.inspect(arg1)
        val num2 = NumberInspector.inspect(arg2)
        if (num1 == null || num2 == null) return null

        return num1 + num2
    }
}

object IsNullFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return arg1 == null
    }
}

object IsNotNullFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return arg1 != null
    }
}

object NumberFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return NumberInspector.inspect(arg1)
    }
}



val scalarfunctionRegistry = mapOf(
        "add" to AddFunction,
        "sum" to AddFunction,
        "idx" to IndexFunction,
        "is_null" to IsNullFunction,
        "is_not_null" to IsNotNullFunction,
        "number" to NumberFunction
)

val aggregatefunctionRegistry = mapOf(
        "sum" to AddFunction
)