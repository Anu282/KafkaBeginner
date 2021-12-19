package com.github.simpleprogram.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithCallback {
    public static void main(String[] args) {
        //System.out.println("hello world");
        //create producer properties
        Logger logger = LoggerFactory.getLogger(ProducerDemowithCallback.class);
        String bootstrapservers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //create a producer record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "hello_world"+ Integer.toString(i));

            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time when record is sent successfully
                    if (e == null) {
                        logger.info("Received new metadata: Topic="
                                + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else
                        System.out.println("Error while producing");
                }
            });
            //flush data

        }
        producer.flush();
        producer.close();
    }
}
