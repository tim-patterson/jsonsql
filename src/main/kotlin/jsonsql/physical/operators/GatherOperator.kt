package jsonsql.physical.operators

import jsonsql.physical.ClosableSequence
import jsonsql.physical.PhysicalOperator
import jsonsql.physical.Tuple
import jsonsql.physical.withClose
import java.util.concurrent.*

class GatherOperator(
        private val sources: List<PhysicalOperator>,
        private val allAtOnce: Boolean // Was used for streaming, the idea being that you don't want to queue waiting for non-terminating streams
): PhysicalOperator() {

    override val columnAliases by lazy { sources.first().columnAliases }

    override fun data(): ClosableSequence<Tuple> {

        val queue = ArrayBlockingQueue<Tuple>(1024)
        val executorPool = if (allAtOnce) Executors.newFixedThreadPool(sources.size) else Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

        val dataSources = sources.map { it.data() }

        fun close() {
            queue.clear()
            executorPool.shutdownNow()
            queue.clear()
            dataSources.forEach { it.close() }
        }

        val tasks = dataSources.map { data ->
            Callable<Unit> {
                val dataIter = data.iterator()
                while (!Thread.interrupted() && dataIter.hasNext()) {
                    queue.put(dataIter.next())
                }
            }
        }

        val futures = tasks.map { executorPool.submit(it) }

        return generateSequence {
            var row: Tuple?
            while (true) {
                row = queue.poll(10, TimeUnit.MILLISECONDS)
                if (row != null) {
                    break
                } else {
                    // We timed out, see if all the workers are done
                    if (futures.all { it.isDone }) {
                        // throw any errors that have occurred
                        futures.map { it.get() }
                        break
                    }
                }
            }
            row
        }.withClose { close() }
    }

    // For explain output
    override fun toString() = "Gather"
}

