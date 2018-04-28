package jsonsql.functions


abstract class OneArgScalarFunction: Function.ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0])
    }

    override fun validateParameterCount(count: Int) = count == 1

    abstract fun execute(arg1: Any?): Any?
}


abstract class TwoArgScalarFunction: Function.ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0], args[1])
    }

    override fun validateParameterCount(count: Int) = count == 2

    abstract fun execute(arg1: Any?, arg2: Any?): Any?
}

object CoalesceFunction: Function.ScalarFunction() {
    override fun validateParameterCount(count: Int) = true

    override fun execute(args: List<Any?>): Any? {
        return args.filterNotNull().firstOrNull()
    }

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

object AndFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val val1 = BooleanInspector.inspect(arg1)
        val val2 = BooleanInspector.inspect(arg2)
        if (val1 == null || val2 == null) return null

        return val1 && val2
    }
}

object OrFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val val1 = BooleanInspector.inspect(arg1)
        val val2 = BooleanInspector.inspect(arg2)
        if (val1 == null || val2 == null) return null

        return val1 || val2
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