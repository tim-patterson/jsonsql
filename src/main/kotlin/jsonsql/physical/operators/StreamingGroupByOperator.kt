package jsonsql.physical.operators

import jsonsql.ast.Ast
import jsonsql.ast.Field
import jsonsql.physical.*
import org.apache.arrow.memory.BufferAllocator
import java.util.concurrent.*

class StreamingGroupByOperator(allocator: BufferAllocator, val expressions: List<Ast.NamedExpr>, val groupByKeys: List<Ast.Expression>, val source: VectorizedPhysicalOperator, val linger: Double, val tableAlias: String?)
    : PhysicalOperator(allocator) {
    private lateinit var compiledGroupExpressions: List<ExpressionExecutor>
    private val executor = Executors.newSingleThreadExecutor()
    private var currentFuture: Future<String?>? = null

    private var nextEmit = System.currentTimeMillis()
    private val dirty = mutableSetOf<String>() // dirty keys


    override fun columnAliases() = expressions.map { Field(tableAlias, it.alias!!) }

    private val cache = mutableMapOf<String, List<AggregateExpressionExecutor>>()

    override fun compile() {
        source.compile()
        compiledGroupExpressions = compileExpressions(groupByKeys, source.columnAliases())
    }

    override fun next(): List<Any?>? {
        while (true) {
            val now = System.currentTimeMillis()
            if(dirty.isNotEmpty() && nextEmit <= now) {
                val key = dirty.first()
                dirty.remove(key)
                return cache[key]?.map { it.getResult() }
            }

            if (currentFuture == null) {
                currentFuture = nextFuture()
            }
            try {
                val result = currentFuture!!.get(now - nextEmit, TimeUnit.MILLISECONDS)
                currentFuture = null
                result?.let {
                    if(dirty.isEmpty()) nextEmit = now + (linger * 1000).toLong()
                    dirty.add(it)
                }
            } catch (e: TimeoutException) {
            }
        }
    }

    override fun close() {
        source.close()
        executor.shutdownNow()
    }

    private fun nextFuture(): Future<String?> {
        return executor.submit (Callable{
            val sourceRow = source.next()
            if (sourceRow != null ) {
                val groupKeys = compiledGroupExpressions.map { it.evaluate(sourceRow) }
                val groupStr = groupKeys.toString()
                val compiledExpressions = cache.computeIfAbsent(groupStr, {
                    compileAggregateExpressions(expressions.map { it.expression }, source.columnAliases())
                })

                compiledExpressions.map { it.processRow(sourceRow) }
                groupStr
            } else {
                null
            }
        })
    }

    override fun toString() = "StreamingGroupBy(${expressions}, groupby - ${groupByKeys})"
}

