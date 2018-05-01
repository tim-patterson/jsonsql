package jsonsql.physical
import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.functions.Function
import jsonsql.functions.functionRegistry

// Scalar stuff
fun compileExpressions(expressions: List<Ast.Expression>, columnAliases: List<Field>): List<ExpressionExecutor> {
    return expressions.map { expression -> compileExpression(expression, columnAliases)}
}

fun compileExpression(expression: Ast.Expression, columnAliases: List<Field>): ExpressionExecutor {
    return when(expression) {
        is Ast.Expression.Identifier -> {
            var idx = columnAliases.indexOf(expression.field)
            if (idx == -1){
                idx = columnAliases.map { it.fieldName }.indexOf(expression.field.fieldName)
            }
            IdentifierExecutor(idx)
        }
        is Ast.Expression.Constant -> ConstantExecutor(expression.value)
        is Ast.Expression.Function -> {
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

fun compileAggregateExpressions(expressions: List<Ast.Expression>, columnAliases: List<Field>): List<AggregateExpressionExecutor> {
    return expressions.map { expression -> compileAggregateExpression(expression, columnAliases)}
}

fun compileAggregateExpression(expression: Ast.Expression, columnAliases: List<Field>): AggregateExpressionExecutor {
    return when(expression) {
        is Ast.Expression.Identifier -> {
            var idx = columnAliases.indexOf(expression.field)
            if (idx == -1){
                idx = columnAliases.map { it.fieldName }.indexOf(expression.field.fieldName)
            }
            IdentifierAggregateExecutor(idx)
        }
        is Ast.Expression.Constant -> ConstantAggregateExecutor(expression.value)
        is Ast.Expression.Function -> {
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