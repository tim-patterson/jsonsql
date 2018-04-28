package jsonsql.physical.operators

import jsonsql.physical.PhysicalOperator
import java.util.concurrent.*

class GatherOperator(val sources: List<PhysicalOperator>): PhysicalOperator() {
    private val queue: LinkedBlockingQueue<List<List<Any?>>> by lazy (::runChildren)
    private lateinit var futures: List<Future<Unit>>
    private lateinit var executorPool: ExecutorService
    private var rowIter: Iterator<List<Any?>> = listOf<List<Any?>>().iterator()
    override fun columnAliases() = sources.first().columnAliases()

    override fun compile() {
        sources.forEach { it.compile() }
    }

    override fun next(): List<Any?>? {
        while (true) {
            if (rowIter.hasNext()) return rowIter.next()

            val rows = queue.poll(10, TimeUnit.MILLISECONDS)
            if (rows != null) {
                rowIter = rows.iterator()
            } else {
                // We timed out, see if all the workers are done
                if (futures.all { it.isDone }) {
                    close()
                    return null
                }
            }
        }
    }

    override fun close() {
        executorPool.shutdownNow()
        queue.clear()
        sources.forEach { it.close() }
    }

    private fun runChildren(): LinkedBlockingQueue<List<List<Any?>>> {
        val queue = LinkedBlockingQueue<List<List<Any?>>>(100)
        executorPool = Executors.newWorkStealingPool()
        val tasks = sources.map { source ->
            Callable<Unit> {
                while (!Thread.interrupted()) {
                    val buf = mutableListOf<List<Any?>>()
                    for (i in 0 until 100) {
                        val row = source.next()
                        if (row != null) {
                            buf.add(row)
                        }else {
                            queue.put(buf)
                            return@Callable
                        }
                    }
                    queue.put(buf)
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

