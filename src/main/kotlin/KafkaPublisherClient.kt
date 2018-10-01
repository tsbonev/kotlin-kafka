import com.github.javafaker.Faker
import com.google.gson.Gson
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.examples.kafkaservice.Event
import io.grpc.examples.kafkaservice.EventServiceGrpc
import io.grpc.stub.StreamObserver
import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * @author Tsvetozar Bonev (tsvetozar.bonev@clouway.com)
 */
class KafkaPublisherClient
internal constructor(private val channel: ManagedChannel) {
    private val eventServiceStub: EventServiceGrpc.EventServiceStub = EventServiceGrpc.newStub(channel)

    /** Construct client connecting to HelloWorld server at `host:port`.  */
    constructor(host: String, port: Int) : this(ManagedChannelBuilder.forAddress(host, port)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build())

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    fun publish(person: Person) {

        val jsonPerson = Gson().toJson(person)

        var request = Event.newBuilder().setContent(jsonPerson).build()

        eventServiceStub.publish(request, object : StreamObserver<Event> {
            override fun onNext(value: Event?) {
                println("This request has been sent: ${request.content}")
            }

            override fun onError(t: Throwable?) {
            }

            override fun onCompleted() {
            }

        })
    }

    companion object {
        /**
         * Greet server. If provided, the first element of `args` is the name to use in the
         * greeting.
         */
        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val client = KafkaPublisherClient("localhost", 50052)

            val faker = Faker()

            try{
                while (true) {
                    val name = readLine()
                    val fakePerson = Person(
                            firstName = name!!,
                            lastName = faker.name().lastName(),
                            birthDate = faker.date().birthday()
                    )
                    client.publish(fakePerson)
                }
            }finally {
                client.shutdown()
            }
        }
    }

    data class Person(
            val firstName: String,
            val lastName: String,
            val birthDate: Date
    )
}
