package jsonsql.filesystems

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.io.InputStream
import java.io.OutputStream
import java.net.URI


object KafkaFileSystem: FileSystem {
    override fun listDir(path: String): List<String> {
        val kafkaUri = URI(path)
        val partition = kafkaUri.path.substringAfterLast(":", "").toIntOrNull()

        return if (partition != null) {
            listOf(path)
        } else {
            val topicName = kafkaUri.path.substringBeforeLast(":").trim('/')
           client(path).use { consumer ->
                consumer.partitionsFor(topicName).map { "$path:${it.partition()}" }
            }
        }

    }

    override fun read(path: String): InputStream {
        val kafkaUri = URI(path)
        val topicName = kafkaUri.path.substringBefore(":").trim('/')
        val partition = kafkaUri.path.substringAfterLast(":", "").toInt()
        val consumer = client(path)
        val topicPartition = TopicPartition(topicName, partition)
        consumer.assign(listOf(topicPartition))
        consumer.seekToEnd(listOf(topicPartition))
        val endOffset = consumer.position(topicPartition)
        consumer.seekToBeginning(listOf(topicPartition))
        var recordIter = listOf<ConsumerRecord<ByteArray,ByteArray>>().iterator()

        return KafkaInputStream({
            var ret: ByteArray? = null
            while (true) {
                if (recordIter.hasNext()) {
                    ret = recordIter.next().value()
                    break
                }
                if (consumer.position(topicPartition) < endOffset) {
                    recordIter = consumer.poll(0).iterator()
                } else {
                    break
                }
            }
            ret
        }, consumer::close)
    }

    override fun write(path: String): OutputStream {
        TODO("not implemented")
    }

    private fun client(path: String): KafkaConsumer<ByteArray, ByteArray> {
        val kafkaUri = URI(path)
        val port = if(kafkaUri.port == -1) 9092  else kafkaUri.port
        return KafkaConsumer(mapOf(
                "bootstrap.servers" to "${kafkaUri.host}:$port",
                "key.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        ))
    }
}

private class KafkaInputStream(val fetchMore: () -> ByteArray?, val closeCb: () -> Unit): InputStream() {
    var buffer: ByteArray = byteArrayOf()
    var offset = 0

    override fun read(): Int {
        while (true) {
            if (offset >= buffer.size) {
                if (!consumeMore()) return -1
            }
            return buffer[offset++].toInt()
        }
    }

    /**
     * Returns true if theres more
     */
    private fun consumeMore(): Boolean {
        buffer = fetchMore() ?: return false
        offset = 0
        return true
    }

    override fun close() {
        closeCb()
    }
}