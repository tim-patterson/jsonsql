package jsonsql.physical

/**
 * Interface used for streaming data out of operators, allows something like a limit to signal that its done with the
 * sequence.
 */
interface ClosableSequence<out T>: Sequence<T>, AutoCloseable

/**
 * Wraps a sequence to turn it into a ClosableSequence, we also autoclose at the end of iteration, no attempt is made to
 * prevent multiple close calls
 */
fun <T> Sequence<T>.withClose(onClose: ()->Unit = {}): ClosableSequence<T> {
    return object: ClosableSequence<T> {
        override fun iterator(): Iterator<T> = object: Iterator<T> {
            private val iter = this@withClose.iterator()
            override fun hasNext() = iter.hasNext().also { if (!it) onClose() }
            override fun next(): T {
                try {
                    return iter.next()
                } catch (e: NoSuchElementException) {
                    onClose()
                    throw e
                }
            }
        }
        override fun close() { onClose() }
    }
}