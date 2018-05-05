package jsonsql.functions

abstract class TwoArgMathFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val num1 = NumberInspector.inspect(arg1)
        var num2 = NumberInspector.inspect(arg2)
        return if (num1 == null || num2 == null) {
            null
        } else {
            execute(num1, num2)
        }
    }

    abstract fun execute(arg1: Double, arg2: Double): Any
}

object AddFunction: TwoArgMathFunction() {
    override fun execute(arg1: Double, arg2: Double) = arg1 + arg2
}

object MinusFunction: TwoArgMathFunction() {
    override fun execute(arg1: Double, arg2: Double) = arg1 - arg2
}

object MultiplyFunction: TwoArgMathFunction() {
    override fun execute(arg1: Double, arg2: Double) = arg1 * arg2
}

object DivideFunction: TwoArgMathFunction() {
    override fun execute(arg1: Double, arg2: Double) = arg1 / arg2
}



object EqFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it == 0 }
}

object NEqFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it != 0 }
}

object GTFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it > 0 }
}

object GTEFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it >= 0 }
}

object LTFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it < 0 }
}

object LTEFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?) = compareValues(arg1, arg2)?.let { it <= 0 }
}