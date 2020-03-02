package jsonsql.physical
import jsonsql.query.Field
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry
import jsonsql.query.Expression

/**
 * The idea behind these functions are to take expressions that are still represented by their query nodes and turn them
 * into something that can be evaluated.
 * One of the optimisations we do in this step is to perform the index lookups once at this compile step instead of for
 * ever tuple. Currently this compilation invoked by the operators.
 *
 * There are two versions of compiled expressions, those for group bys (many rows) -> 1 or those for normal scalar
 * expressions(ie in the select, where and join-on clauses) which are 1-1.
 *
 * This gives the (upstream) operators some flexibility as to what order etc the want to order the fields in their tuples
 * instead of baking all that logic into the tree builder and hence spreading logic around.
 */

// Scalar stuff
fun compileExpressions(expressions: List<Expression>, columnAliases: List<Field>): List<ExpressionExecutor> {
    return expressions.map { expression -> compileExpression(expression, columnAliases)}
}

fun compileExpression(expression: Expression, columnAliases: List<Field>): ExpressionExecutor {
    return when(expression) {
        is Expression.Identifier -> {
            var idx = columnAliases.indexOf(expression.field)
            assert(idx != -1) { "Identifier ${expression.field} not found in $columnAliases" }
            IdentifierExecutor(idx)
        }
        is Expression.Constant -> ConstantExecutor(expression.value)
        is Expression.Function -> {
            val function = functionRegistry[expression.functionName]!!
            FunctionExecutor(function as Function.ScalarFunction, compileExpressions(expression.parameters, columnAliases))
        }
    }
}

abstract class ExpressionExecutor {
    abstract fun evaluate(row: List<Any?>): Any?
}

class ConstantExecutor(val constant: Any?): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = constant
}

class IdentifierExecutor(val offset: Int): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = row[offset]
}

class FunctionExecutor(val function: Function.ScalarFunction, val args: List<ExpressionExecutor>): ExpressionExecutor() {
    override fun evaluate(row: List<Any?>) = function.execute(args.map { it.evaluate(row) })
}


// Aggregate stuff

fun compileAggregateExpressions(expressions: List<Expression>, columnAliases: List<Field>): List<AggregateExpressionExecutor> {
    return expressions.map { expression -> compileAggregateExpression(expression, columnAliases)}
}

fun compileAggregateExpression(expression: Expression, columnAliases: List<Field>): AggregateExpressionExecutor {
    return when(expression) {
        is Expression.Identifier -> {
            var idx = columnAliases.indexOf(expression.field)
            IdentifierAggregateExecutor(idx)
        }
        is Expression.Constant -> ConstantAggregateExecutor(expression.value)
        is Expression.Function -> {
            val function = functionRegistry[expression.functionName]!!
            when (function) {
                is Function.ScalarFunction -> ScalarFunctionAggregateExecutor(function, compileAggregateExpressions(expression.parameters, columnAliases))
                is Function.AggregateFunction -> AggregateFunctionExecutor(function, compileExpressions(expression.parameters, columnAliases))
            }
        }
    }
}

abstract class AggregateExpressionExecutor {
    abstract fun processRow(row: List<Any?>)
    abstract fun getResult(): Any?
    abstract fun reset()
}


// Expose a scalar function as an aggregate
class ScalarFunctionAggregateExecutor(val function: Function.ScalarFunction, val args: List<AggregateExpressionExecutor>) : AggregateExpressionExecutor() {
    override fun processRow(row: List<Any?>) {
        args.map { it.processRow(row) }
    }

    override fun getResult(): Any? {
        return function.execute(args.map { it.getResult() })
    }

    override fun reset() {
        args.map { it.reset() }
    }
}

class IdentifierAggregateExecutor(val offset: Int): AggregateExpressionExecutor() {
    private var cached: Any? = null
    override fun processRow(row: List<Any?>) {
        cached = row[offset]
    }
    override fun getResult() = cached
    override fun reset() {
        cached = null
    }
}

class ConstantAggregateExecutor(val constant: Any?): AggregateExpressionExecutor() {
    override fun processRow(row: List<Any?>) {}
    override fun getResult() = constant
    override fun reset() {}
}

// Aggregate Function
class AggregateFunctionExecutor(val function: Function.AggregateFunction, val args: List<ExpressionExecutor>): AggregateExpressionExecutor() {
    private var executor = function.executor()
    // Everything under a aggregate should be scalar
    override fun processRow(row: List<Any?>) = executor.processInput(args.map { it.evaluate(row)})
    override fun getResult() = executor.getResult()

    override fun reset() {
        executor = function.executor()
    }
}