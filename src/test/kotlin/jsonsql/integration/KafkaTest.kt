package jsonsql.integration

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe


@RunWith(JUnitPlatform::class)
object KafkaTest: Spek({

    val sharedKafkaTestResource = SharedKafkaTestResource()
    beforeGroup {
        sharedKafkaTestResource.beforeAll(null)
        // First Write the records into kafka
        val kafkaUtils = sharedKafkaTestResource.kafkaTestUtils
        kafkaUtils.createTopic("myTopic", 2, 1)
        val producer = kafkaUtils.getKafkaProducer(StringSerializer::class.java, StringSerializer::class.java)
        val f1 = producer.send(ProducerRecord("myTopic", 0, "1", "{\"foo\": 1, \"bar\":\"abc\"}"))
        val f2 = producer.send(ProducerRecord("myTopic", 1, "2", "{\"foo\": 2, \"bar\":\"def\"}"))
        f1.get()
        f2.get()
    }
    afterGroup { sharedKafkaTestResource.afterAll(null) }

    describe("Kafka Reads") {
        it("works") {
            val connectionString = sharedKafkaTestResource.kafkaConnectString.removePrefix("PLAINTEXT://")
            testQuery("SELECT foo, bar FROM json 'kafka://$connectionString/myTopic' ORDER BY foo;", """
                1.0 | abc
                2.0 | def
            """.trimIndent())
        }
    }
}) {
}