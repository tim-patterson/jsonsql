package jsonsql.physical.operators

import jsonsql.filesystems.FileSystem
import jsonsql.physical.*
import java.lang.Exception
import java.util.concurrent.*

/**
 * Operator that runs certain downstream operations in parallel when we're scanning directories
 */
class GatherOperator(
        private val source: PhysicalOperator,
        private val rootPath: String
): PhysicalOperator() {

    override val columnAliases by lazy { source.columnAliases }

    override fun data(context: ExecutionContext): ClosableSequence<Tuple> {
        val queue = ArrayBlockingQueue<Tuple>(1024)
        val executorPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

        var dataSources: List<ClosableSequence<Tuple>> = listOf()

        fun close() {
            queue.clear()
            executorPool.shutdownNow()
            queue.clear()
            dataSources.forEach { it.close() }
        }

        // Due to alot of the work being done here outside of the returned sequence itself we need to be able to catch
        // any exceptions(Ie ctrl-c ) and clean up after ourselves.
        try {

            val files = FileSystem.listDir(rootPath).toList()
            // In the case where the file passed in is a singular file or a directory with a single file we'll just
            // short this operator
            if (files.size <= 1) {
                return source.data(context)
            }

            dataSources = files.map {
                val subContext = context.copy(pathOverrides = context.pathOverrides + mapOf(rootPath to it["path"] as String))
                source.data(subContext)
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
        } catch (e: Exception) {
            close()
            throw e
        }
    }

    // For explain output
    override fun toString() = "Gather"
}

