import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.examples.kafkaservice.Event
import io.grpc.examples.kafkaservice.EventServiceGrpc
import io.grpc.examples.kafkaservice.Topic
import io.grpc.stub.StreamObserver
import java.util.concurrent.TimeUnit

/**
 * @author Tsvetozar Bonev (tsvetozar.bonev@clouway.com)
 */
class KafkaSubscriberClient internal constructor(private val channel: ManagedChannel) {
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

    fun subscribe(topicName: String) {
        val topic = Topic.newBuilder().setTopicName(topicName).build()

        eventServiceStub.subscribe(topic, object : StreamObserver<Event> {
            override fun onNext(value: Event?) {
                println("Record read from topic: ${value!!.content}")
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
            val client = KafkaSubscriberClient("localhost", 50052)

            val topicName = "people"

            try{
                client.subscribe(topicName)
                while (true){

                }
            }finally{
                client.shutdown()
            }
        }
    }
}