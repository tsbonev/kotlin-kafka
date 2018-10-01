import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

/**
 * @author Tsvetozar Bonev (tsvetozar.bonev@clouway.com)
 */
class KafkaBuilder {
    companion object {
        fun createProducer(brokers: String) : Producer<String, String>{
            val props = Properties()
            props["bootstrap.servers"] = brokers
            props["key.serializer"] = StringSerializer::class.java.canonicalName
            props["value.serializer"] = StringSerializer::class.java.canonicalName
            return KafkaProducer<String, String>(props)
        }

        fun createConsumer(brokers: String): Consumer<String, String> {
            val props = Properties()
            props["bootstrap.servers"] = brokers
            props["group.id"] = "person-processor"
            props["key.deserializer"] = StringDeserializer::class.java
            props["value.deserializer"] = StringDeserializer::class.java
            return KafkaConsumer<String, String>(props)
        }
    }
}