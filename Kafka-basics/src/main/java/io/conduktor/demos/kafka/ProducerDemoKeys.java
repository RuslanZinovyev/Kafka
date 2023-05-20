package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static final String TOPIC = "demo_java";

    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka producer:");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int x = 0; x < 3; x++) {
            for (int i = 0; i < 100; i++) {
                String key = "id-" + i;
                String value = "This is Kafka message: " + i;

                // create a Producer Record
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // send data
                log.info("The message was sent");
                producer.send(record, (metadata, exception) -> {
                    // executes every time a record successfully send or an exception is thrown
                    if (exception == null) {
                        // record was successfully sent
                        log.info("Key: {} | Partition: {} ", key, metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }
            Thread.sleep(500);
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
