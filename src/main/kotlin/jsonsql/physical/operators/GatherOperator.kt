package jsonsql.physical.operators

import jsonsql.physical.PhysicalOperator
import java.util.concurrent.*

class GatherOperator(val sources: List<PhysicalOperator>, val allAtOnce: Boolean): PhysicalOperator() {
    private val queue: BlockingQueue<List<Any?>> by lazy (::runChildren)
    private lateinit var futures: List<Future<Unit>>
    private lateinit var executorPool: ExecutorService

    override fun columnAliases() = sources.first().columnAliases()

    override fun compile() {
        sources.forEach { it.compile() }
    }

    override fun next(): List<Any?>? {
        while (true) {
            val row = queue.poll(10, TimeUnit.MILLISECONDS)
            if (row != null) {
                return row
            } else {
                // We timed out, see if all the workers are done
                if (futures.all { it.isDone }) {
                    close()
                    // throw any errors that have occurred
                    futures.map { it.get() }
                    return null
                }
            }
        }
    }

    override fun close() {
        queue.clear()
        executorPool.shutdownNow()
        queue.clear()
        sources.forEach { it.close() }
    }

    private fun runChildren(): BlockingQueue<List<Any?>> {
        val queue = ArrayBlockingQueue<List<Any?>>(1024)
        executorPool = if (allAtOnce) Executors.newFixedThreadPool(sources.size) else Executors.newWorkStealingPool()
        val tasks = sources.map { source ->
            Callable<Unit> {
                while (!Thread.interrupted()) {
                    val row = source.next()
                    if (row != null) {
                        queue.put(row)
                    }else {
                        return@Callable
                    }
                }
            }
        }
        futures = tasks.map { executorPool.submit(it) }

        return queue
    }


    // For explain output
    override fun toString() = "Gather"
    override fun children() = listOf(sources.first())
}

