package com.lopez.rafael;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {
    private static Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class);

    public static void main(String[] args) {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // From version 2.4 and later of Apache Kafka, the default partitioning strategy has been changed for
        // records with a null key whereby sticky partitioning is the default behavior. You can change the
        // partitioner setting for a producer to round robin.
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++) {
            // Create ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord("first_topic", "hello world " + i);

            // Send data - This is asynchronous, if you don't flush (or close) the producer, the app will finish
            // before it had a chance to actually send the data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is successfully sent, or an exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
