package com.example.kafkademo.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerSample {
    private static final String TOPIC_NAME = "defaultTopic";

    public static void main(String[] args) throws Exception {
        producerSend();
//        producerSynSend();
//        producerSynSendWithCallback();
//        producerSynSendWithCallbackAndPartiton();
    }

    /**
     * 异步发送
     */
    public static void producerSend() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "112.126.65.55:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        producer主对象
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            kafkaProducer.send(producerRecord);
        }
//        关闭通道
        kafkaProducer.close();

    }

    /**
     * 异步阻塞
     */
    public static void producerSynSend() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "112.126.65.55:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        producer主对象
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.println("partiton:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
        }
//        关闭通道
        kafkaProducer.close();

    }

    /**
     * 异步带回调
     */
    public static void producerSynSendWithCallback() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "112.126.65.55:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        producer主对象
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("partiton:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
                }
            });
        }
//        关闭通道
        kafkaProducer.close();

    }

    /**
     * 异步带回调、自定义分区
     */
    public static void producerSynSendWithCallbackAndPartiton() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "112.126.65.55:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafkademo.producer.PartitionSample");

//        producer主对象
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord producerRecord = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("partiton:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
                }
            });
        }
//        关闭通道
        kafkaProducer.close();

    }
}
