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
inline fun <T> Sequence<T>.withClose(crossinline onClose: ()->Unit = {}): ClosableSequence<T> {
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

/**
 * Because the .data methods of our operators maybe be called in one thread but we intend to consume in another
 * it's important for us to ensure that our sequences are truly lazy and that no real work is done until the iterator
 * method is called.
 * This lazySeq ensures that, it simply delays the block until the iterator is called.
 */
inline fun <T> lazySeq(crossinline init: ()->Sequence<T> ): Sequence<T> {
    return object: Sequence<T> {
        override fun iterator(): Iterator<T> {
            return init().iterator()
        }
    }
}