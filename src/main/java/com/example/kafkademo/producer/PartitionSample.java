package com.example.kafkademo.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 */
public class PartitionSample implements Partitioner {

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String keyStr = o + "";
        String substring = keyStr.substring(4);
        int keyInt = Integer.parseInt(substring);
        System.out.println("keyStr:" + keyStr + " keyInt:" + keyInt);
        return keyInt % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
