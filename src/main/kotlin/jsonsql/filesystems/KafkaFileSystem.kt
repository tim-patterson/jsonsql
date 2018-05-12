package jsonsql.filesystems

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.net.URI


object KafkaFileSystem: EventFileSystem() {
    override fun listDir(path: String): List<Map<String, Any?>> {
        val kafkaUri = URI(path)
        val topicName = kafkaUri.path.substringBeforeLast(":").trim('/')
        val partition = kafkaUri.path.substringAfterLast(":", "").toIntOrNull()

        return if (partition != null) {
            listOf(mapOf(
                    "path" to path,
                    "topic" to topicName,
                    "partition" to partition
            ))
        } else {

           client(path).use { consumer ->
                consumer.partitionsFor(topicName).map {
                    mapOf(
                            "path" to "$path:${it.partition()}",
                            "topic" to topicName,
                            "partition" to it.partition()
                    )
                }
            }
        }

    }

    override fun read(path: String, terminating: Boolean): EventReader {
        val consumer = client(path)
        val topicPartitions = listDir(path).map {
            TopicPartition(it["topic"] as String, it["partition"] as Int)
        }
        consumer.assign(topicPartitions)
        consumer.seekToEnd(topicPartitions)
        val endOffsets = topicPartitions.zip(topicPartitions.map { consumer.position(it) })
        consumer.seekToBeginning(topicPartitions)

        var records: Iterator<ByteArray> = listOf<ByteArray>().iterator()

        return object: EventReader {
            override fun next(): ByteArray? {
                while (true) {
                    if (records.hasNext()) {
                        return records.next()
                    }

                    if (terminating && endOffsets.all { (topicPartition, offset) -> consumer.position(topicPartition) >= offset }) {
                        return null
                    }

                    records = consumer.poll(1000).iterator().asSequence().map {
                        it.value()
                    }.iterator()
                }
            }

            override fun close() {
                consumer.close()
            }
        }
    }

    override fun write(path: String): EventWriter {
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
