package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class.getSimpleName());
    public static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        log.info("Kafka producer:");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "This is Kafka message: " + i);

            // send data
            log.info("The message was sent");
            producer.send(record, (metadata, exception) -> {
                // executes every time a record successfully send or an exception is thrown
                if (exception == null) {
                    // record was successfully sent
                    log.info("Received new metadata \n Topic: {} \n Partition: {} \n Offsets: {} \n Timestamp: {}",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    log.error("Error while producing", exception);
                }
            });
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
