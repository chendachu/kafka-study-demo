package com.example.kafkademo.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;


import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminSample {
    private static final String TOPIC_NAME = "defaultTopic";

    public static void main(String[] args) throws Exception {
//        createTopic();
//        listTopic();
//        listTopic2();
//        delTopic();
//        listTopic2();
//        createTopic();
//        describeTopic();
//        alterConfig();
//        increPartition(2);
//        describeConfig();

    }

    /**
     * 创建adminclient
     *
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "112.126.65.55:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建topic
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        try {
            topics.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取所有topic
     *
     * @throws Exception
     */
    public static void listTopic() throws Exception {
        AdminClient adminClient = adminClient();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(item -> {
            System.out.println(item.name());
        });

    }

    /**
     * 获取所有topic包含内部topic
     *
     * @throws Exception
     */
    public static void listTopic2() throws Exception {
        AdminClient adminClient = adminClient();
        // 获取内部topic
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(System.out::println);
    }


    /**
     * 删除topic
     */
    public static void delTopic() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        System.out.println(deleteTopicsResult.all().get());

    }

    /**
     * 描述topic
     */
    public static void describeTopic() throws Exception {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entrie) -> {
            System.out.println("name:" + entrie.getKey() + "value:" + entrie.getValue());
        });
    }

    /**
     * 查看topic配置信息
     */
    public static void describeConfig() throws Exception {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        Set<Map.Entry<ConfigResource, Config>> entries = configResourceConfigMap.entrySet();
        entries.stream().forEach((entrie) -> {
            System.out.println("ConfigResource:" + entrie.getKey() + "value:" + entrie.getValue());
        });
    }

    /**
     * 修改topic配置
     */
    public static void alterConfig() {
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> map = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));
        map.put(configResource, config);
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(map);
    }

    public static void increPartition(int num) throws Exception {
        AdminClient adminClient = adminClient();
        NewPartitions newPartitions = NewPartitions.increaseTo(num);
        Map<String, NewPartitions> stringNewPartitionsHashMap = new HashMap<>();
        stringNewPartitionsHashMap.put(TOPIC_NAME, newPartitions);

        CreatePartitionsResult partitions = adminClient.createPartitions(stringNewPartitionsHashMap);
        partitions.all().get();
    }


}
