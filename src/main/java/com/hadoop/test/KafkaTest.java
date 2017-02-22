package com.hadoop.test;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class KafkaTest {
    private static  String brokerList="";
    private static String topicName="";

    public static void main(String[] args) {

        if(args.length < 2) {
            System.out.println("Require parameters <broker-list> <topic-name>");
            System.exit(1);
        } else {
            brokerList=args[0];
            topicName=args[1];
        }

        KafkaTest testInstance = new KafkaTest();

        System.out.println("++++++++ Starting produce message ++++++++");
        testInstance.produceMessage();

        System.out.println("++++++++ Starting consume message ++++++++");
        testInstance.consumeMessage();



    }

    public void produceMessage() {
        Properties producerConfig = new Properties();

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // specify the protocol for SSL Encryption
        producerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(producerConfig);

        TestCallback callback = new TestCallback();
        Random rnd = new Random();
        for (long i = 0; i < 30 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                    topicName, "key-" + i, "message-"+i );
            producer.send(data, callback);
        }

        producer.close();

    }

    public void consumeMessage() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // specify the protocol for SSL Encryption
        consumerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
        TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
        consumer.subscribe(Collections.singletonList(topicName), rebalanceListener);

        int i =0;

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            if( records.isEmpty()) {
                i++;
                if (i>=10) {
                    consumer.commitSync();
                    consumer.close();
                    System.out.println("No more messages, closing consumer...")
                    System.exit(0);
                }
            } else {
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                i=0;
            }

            consumer.commitSync();
        }

    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

    private static class TestConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }

}