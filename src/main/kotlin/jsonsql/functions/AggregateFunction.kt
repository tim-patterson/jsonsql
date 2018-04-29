package jsonsql.functions

abstract class AggregateFunctionExecutor {
    abstract fun processInput(args: List<Any?>)
    abstract fun getResult(): Any?
}

object CountFunction: Function.AggregateFunction() {
    override fun validateParameterCount(count: Int) = count <= 1
    override fun executor() = object: AggregateFunctionExecutor() {
        private var count = 0
        override fun processInput(args: List<Any?>) {
            if (args.isEmpty() || args.first() != null) count++
        }

        override fun getResult() = count
    }
}


object SumFunction: Function.AggregateFunction() {
    override fun validateParameterCount(count: Int) = count == 1
    override fun executor() = object: AggregateFunctionExecutor() {
        private var sum: Double = 0.0
        override fun processInput(args: List<Any?>) {
            NumberInspector.inspect(args.first())?.let { sum += it }
        }

        override fun getResult() = sum
    }
}

object MaxFunction: Function.AggregateFunction() {
    override fun validateParameterCount(count: Int) = count == 1
    override fun executor() = object: AggregateFunctionExecutor() {
        private var maxValue: Any? = null
        override fun processInput(args: List<Any?>) {
            if (compareValues(args[0], maxValue) > 0) {
                maxValue = args[0]
            }
        }

        override fun getResult() = maxValue
    }
}

object MinFunction: Function.AggregateFunction() {
    override fun validateParameterCount(count: Int) = count == 1
    override fun executor() = object: AggregateFunctionExecutor() {
        private var minValue: Any? = null
        override fun processInput(args: List<Any?>) {
            if (minValue == null) {
                minValue = args[0]
            } else if (args[0] != null && compareValues(args[0], minValue) < 0) {
                minValue = args[0]
            }
        }

        override fun getResult() = minValue
    }
}



object MaxRowFunction: Function.AggregateFunction() {
    override fun validateParameterCount(count: Int) = count == 2
    override fun executor() = object: AggregateFunctionExecutor() {
        private var row: Any? = null
        private var maxKey: Any? = null

        override fun processInput(args: List<Any?>) {
            println("${args[0]} <!> $maxKey = ${compareValues(args[0], maxKey)}")
            if (compareValues(args[0], maxKey) > 0) {
                maxKey = args[0]
                row = args[1]
            }
        }

        override fun getResult() = row
    }
}