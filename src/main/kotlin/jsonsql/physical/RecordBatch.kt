package jsonsql.physical

import org.apache.arrow.vector.FieldVector

/**
 * Used for passing data from operator to operator.
 * Sink operators are responsible for releasing vectors once they're done with them.
 * In order to support decomposing joins into a join followed by a select we need to support
 * qualified field names...
 */
data class RecordBatch(
        val vectors: List<FieldVector>
) {
    val vectorsByName = vectors.associateBy { it.field.name }
            .also { assert(it is LinkedHashMap) } // Part of the contract here is that this is ordered
    val recordCount = vectors.first().valueCount

    /**
     * Release all vectors in this batch
     */
    fun releaseAll() = vectors.forEach(FieldVector::clear)
}