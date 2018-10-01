import com.google.gson.Gson
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.examples.kafkaservice.Event
import io.grpc.examples.kafkaservice.EventServiceGrpc
import io.grpc.examples.kafkaservice.Topic
import io.grpc.stub.StreamObserver
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.IOException
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.logging.Level
import java.util.logging.Logger

/**
 * @author Tsvetozar Bonev (tsvetozar.bonev@clouway.com)
 */
class KafkaServiceServer {

    private var server: Server? = null

    @Throws(IOException::class)
    private fun start() {
        /* The port on which the server should run */
        val port = 50052
        server = ServerBuilder.forPort(port)
                .addService(ChatImpl())
                .build()
                .start()
        logger.log(Level.INFO, "Server started, listening on {0}", port)
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down")
                this@KafkaServiceServer.stop()
                System.err.println("*** server shut down")
            }
        })
    }

    private fun stop() {
        server?.shutdown()
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    @Throws(InterruptedException::class)
    private fun blockUntilShutdown() {
        server?.awaitTermination()
    }

    internal class ChatImpl : EventServiceGrpc.EventServiceImplBase() {

        private val producer = KafkaBuilder.createProducer("localhost:9092")
        private val consumer = KafkaBuilder.createConsumer("localhost:9092")


        override fun publish(request: Event?, responseObserver: StreamObserver<Event>?) {
            logger.info("Next value taken: $request")
            producer.send(ProducerRecord("people", request!!.content)).get()
            responseObserver!!.onNext(request)
        }

        override fun subscribe(request: Topic?, responseObserver: StreamObserver<Event>?) {
            consumer.subscribe(listOf(request!!.topicName))

            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                logger.info("Received ${records.count()} records")

                records.iterator().forEach {

                    val event = Event.newBuilder().setContent(it.value()).build()

                    responseObserver!!.onNext(event)
                }
            }
        }
    }

    companion object {
        private val logger = Logger.getLogger(KafkaServiceServer::class.java.name)

        /**
         * Main launches the server from the command line.
         */
        @Throws(IOException::class, InterruptedException::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val server = KafkaServiceServer()
            server.start()
            server.blockUntilShutdown()
        }
    }

}