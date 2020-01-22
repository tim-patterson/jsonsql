package jsonsql.physical.operators

import jsonsql.physical.PhysicalOperator
import jsonsql.physical.VectorizedPhysicalOperator
import org.apache.arrow.memory.BufferAllocator
import java.util.concurrent.*

class GatherOperator(allocator: BufferAllocator, val sources: List<VectorizedPhysicalOperator>, val allAtOnce: Boolean): PhysicalOperator(allocator) {
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
        executorPool = if (allAtOnce) Executors.newFixedThreadPool(sources.size) else Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)
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

    override fun toString() = "Gather"
}

