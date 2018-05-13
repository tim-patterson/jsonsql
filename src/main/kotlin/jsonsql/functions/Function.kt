package jsonsql.functions;

sealed class Function {
    abstract fun validateParameterCount(count: Int) : Boolean

    abstract class ScalarFunction: Function() {
        abstract fun execute(args: List<Any?>): Any?
    }

    abstract class AggregateFunction: Function() {
        abstract fun executor(): AggregateFunctionExecutor
    }
}

val functionRegistry = mapOf<String, Function>(
        //math
        "add" to AddFunction,
        "minus" to MinusFunction,
        "multiply" to MultiplyFunction,
        "divide" to DivideFunction,
        "gt" to GTFunction,
        "gte" to GTEFunction,
        "lt" to LTFunction,
        "lte" to LTEFunction,
        "equal" to EqFunction,
        "not_equal" to NEqFunction,
        // util
        "idx" to IndexFunction,
        "is_null" to IsNullFunction,
        "is_not_null" to IsNotNullFunction,
        "number" to NumberFunction,
        "or" to OrFunction,
        "and" to AndFunction,
        "coalesce" to CoalesceFunction,
        "tumble" to TumbleFunction,

        // Aggregate Functions
        "count" to CountFunction,
        "sum" to SumFunction,
        "min" to MinFunction,
        "max" to MaxFunction,
        "max_row" to MaxRowFunction,
        "count_distinct" to CountDistinctFunction
)