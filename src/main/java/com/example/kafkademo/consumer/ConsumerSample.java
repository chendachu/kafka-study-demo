package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerSample {
    private static final String TOPIC_NAME = "defaultTopic";

    public static void main(String[] args) {
//        commitOffset();
//        commitOffsetWithPartiton();
        consumerPartion();
    }


    /**
     * 自动提交offset
     */
    public static void autoCommitOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "112.126.65.55:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //每隔多少秒拉去消息
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("消费消息:" + "partiton = %s, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
        }
    }

    /**
     * 手动提交offset
     */
    public static void commitOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "112.126.65.55:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //每隔多少秒拉去消息
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("消费消息:" + "partiton = %s, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                // TODO: 2020/10/7 业务处理入库 失败回滚
            }
            // 消费成功 手动提交offset
            consumer.commitAsync();
        }

    }

    /**
     * 手动提交单个分区的offset
     */
    public static void commitOffsetWithPartiton() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "112.126.65.55:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //每隔多少秒拉去消息
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition topicPartition : partitions) {
                List<ConsumerRecord<String, String>> records1 = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("消费消息:" + "partiton = %s, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    // TODO: 2020/10/7 业务处理入库 失败回滚
                }
                long offset = records1.get(records1.size() - 1).offset();
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
                HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap = new HashMap<>();
                topicPartitionOffsetAndMetadataHashMap.put(topicPartition, offsetAndMetadata);
                System.out.println("-----------------partiton offset" + offsetAndMetadata + "--------------------");
                //对单个partion提交offset
                consumer.commitSync(topicPartitionOffsetAndMetadataHashMap);
            }
        }

    }

    /**
     * 消费某个topic某个分区并提交offset
     */
    public static void consumerPartion() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "112.126.65.55:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        consumer.assign(Arrays.asList(p0));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //每隔多少秒拉去消息
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition topicPartition : partitions) {
                List<ConsumerRecord<String, String>> records1 = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("消费消息:" + "partiton = %s, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    // TODO: 2020/10/7 业务处理入库 失败回滚
                }
                long offset = records1.get(records1.size() - 1).offset();
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
                HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap = new HashMap<>();
                topicPartitionOffsetAndMetadataHashMap.put(topicPartition, offsetAndMetadata);
                System.out.println("-----------------partiton offset" + offsetAndMetadata + "--------------------");
                //对单个partion提交offset
                consumer.commitSync(topicPartitionOffsetAndMetadataHashMap);
            }
        }

    }


}
