package com.lopez.rafael;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

// You can run multiple instances of this class, to see how Consumers automatically join the group and get assigned a
// partition. Or how, once the consumer is stopped, the running ones get re-assigned
public class ConsumerAssignSeekDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        // Create Consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // We don't want to subscribe to topics. Instead, we want to read from wherever we want.
        // Assign and Seek are mostly used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        // Here we are basically saying: this consumer should read from this specific partition
        // and also, from offset 15

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        // Poll for new data
        while(keepOnReading) {
            // When you poll. you need to give it a timout for how long it should try to get data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (numberOfMessagesToRead <= numberOfMessagesReadSoFar) {
                    keepOnReading = false; // to exit the while loop
                    break; // to exit the foor loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
