package com.github.marcusvieira.kafka.tutorial1;

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
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class ConsumerDemoAssingSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssingSeek.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        //seek
        long offsetToSeek = 15L;
        consumer.seek(topicPartition, offsetToSeek);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesRead = 0;

        //poll data
        while (numberOfMessagesRead <= numberOfMessagesToRead) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Metadata \n" +
                        "Topic: " + record.topic() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Timestamp: " + record.timestamp() + "\n"
                );
                numberOfMessagesRead++;
            }
        }
    }
}
