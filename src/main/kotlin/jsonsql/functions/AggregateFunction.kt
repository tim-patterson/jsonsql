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