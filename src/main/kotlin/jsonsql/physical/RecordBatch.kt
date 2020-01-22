package jsonsql.physical

import org.apache.arrow.vector.FieldVector

data class RecordBatch(
        val vectors: List<FieldVector>
) {
    val vectorsByName = vectors.associateBy { it.field.name }
            .also { assert(it is LinkedHashMap) } // Part of the contract here is that this is ordered
    val recordCount = vectors.first().valueCount
}