import com.github.javafaker.Faker
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.Date
import java.util.Properties

/**
 * @author Tsvetozar Bonev (tsvetozar.bonev@clouway.com)
 */
class KafkaProducerTest {

    private val logger = LoggerFactory.getLogger(KafkaProducerTest::class.java)

    private val faker = Faker()

    private val brokers = "localhost:9092"

    private val peopleTopic = "people"

    private val agesTopic = "people-ages"

    private val fakePerson = Person(
            firstName = faker.name().firstName(),
            lastName = faker.name().lastName(),
            birthDate = faker.date().birthday()
    )

    private val fakePersonJson = Gson().toJson(fakePerson)

    private val producer = createProducer(brokers)
    private val consumer = createConsumer(brokers)

    @Test
    fun sendMessage() {
        val futureResult = producer.send(ProducerRecord(peopleTopic, fakePersonJson))

        futureResult.get()
    }

    @Test
    fun listenForMessage() {
        consumer.subscribe(listOf(peopleTopic))

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} records")

            records.iterator().forEach {
                val personJson = it.value()
                logger.debug("JSON data: $personJson")

                val person = Gson().fromJson(personJson, Person::class.java)
                logger.debug("Person: $person")

                val birthDateLocal = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
                val age = Period.between(birthDateLocal, LocalDate.now()).years
                logger.debug("Age: $age")

                val future = producer
                        .send(ProducerRecord(agesTopic,
                                "${person.firstName} ${person.lastName}",
                                "$age"))
                future.get()
            }

        }
    }

}

private fun createProducer(brokers: String): Producer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = brokers
    props["key.serializer"] = StringSerializer::class.java.canonicalName
    props["value.serializer"] = StringSerializer::class.java.canonicalName
    return KafkaProducer<String, String>(props)
}

private fun createConsumer(brokers: String): Consumer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = brokers
    props["group.id"] = "person-processor"
    props["key.deserializer"] = StringDeserializer::class.java
    props["value.deserializer"] = StringDeserializer::class.java
    return KafkaConsumer<String, String>(props)
}

data class Person(
        val firstName: String,
        val lastName: String,
        val birthDate: Date
)