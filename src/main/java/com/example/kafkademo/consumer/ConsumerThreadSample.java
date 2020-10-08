package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多线程处理
 *
 * kafka consumer不是线程安全的
 *
 *
 *
 */
public class ConsumerThreadSample {
    private static final String TOPIC_NAME = "defaultTopic";

    public static void main(String[] args) throws Exception {
        ConsumerRunner consumerRunner = new ConsumerRunner();
        Thread thread = new Thread(consumerRunner);
        thread.start();
        TimeUnit.SECONDS.sleep(10);
        consumerRunner.shutDown();


    }


    public static class ConsumerRunner implements Runnable {
        private final AtomicBoolean cloased = new AtomicBoolean(false);
        private KafkaConsumer consumer;


        public ConsumerRunner() {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", "112.126.65.55:9092");
            props.setProperty("group.id", "test");
            props.setProperty("enable.auto.commit", "false");
            props.setProperty("auto.commit.interval.ms", "1000");
            props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);
            consumer.assign(Arrays.asList(p0, p1));
        }

        @Override
        public void run() {
            try {
                while (!cloased.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
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
            } catch (WakeupException e) {
                if (!cloased.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }

        public void shutDown() {
            cloased.set(true);
            consumer.wakeup();
        }
    }
}
