package com.github.marcusvieira.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "marcus test " + i;
            String key = "id_" + i;

            //create a record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("KEY: " + key);

            //send a message asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );

                    } else {
                        logger.error("Erro when send a message", e);
                    }
                }
            }).get();//become the send synchronous do not do this in ptoduction
        }

        //need to flush for send a message before close the program
        producer.flush();
        //close the producer
        producer.close();

    }
}
