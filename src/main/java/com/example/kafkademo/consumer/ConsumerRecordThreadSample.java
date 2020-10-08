package com.example.kafkademo.consumer;

import ch.qos.logback.core.util.TimeUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池处理record
 */
public class ConsumerRecordThreadSample {
    private static final String TOPIC_NAME = "defaultTopic";

    public static void main(String[] args) throws InterruptedException {
        String brokerList = "112.126.65.55:9092";
        String groupId = "test";
        ConsumerExecutor consumerExecutor = new ConsumerExecutor(brokerList, groupId);
        consumerExecutor.excute(5);
        TimeUnit.SECONDS.sleep(10);
        consumerExecutor.shutDown();
    }

    public static class ConsumerExecutor {
        private final KafkaConsumer<String, String> consumer;
        private ExecutorService executorService;

        public ConsumerExecutor(String brokerList, String groupId) {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", brokerList);
            props.setProperty("group.id", groupId);
            props.setProperty("enable.auto.commit", "false");
            props.setProperty("auto.commit.interval.ms", "1000");
            props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(TOPIC_NAME));
        }

        public void excute(int workerNum) {
            executorService = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MICROSECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record : records) {
                    executorService.submit(new RecordWorker(record));
                }
            }
        }

        public void shutDown() {
            if (consumer != null) {
                consumer.close();
            }
            if (executorService != null) {
                executorService.shutdown();
            }
            try {
                if (executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Timeout ignore for this case");
                }
            } catch (InterruptedException e) {
                System.out.println("Other thread interrupted this shutdown,ignore for this case");
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class RecordWorker implements Runnable {
        private ConsumerRecord<String, String> consumerRecord;

        public RecordWorker(ConsumerRecord consumerRecord) {
            this.consumerRecord = consumerRecord;
        }

        @Override
        public void run() {
            System.out.println("Thread name:" + Thread.currentThread().getName());
            System.out.printf("消费消息:" + "partiton = %s, offset = %d, key = %s, value = %s%n", consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());

        }
    }
}
