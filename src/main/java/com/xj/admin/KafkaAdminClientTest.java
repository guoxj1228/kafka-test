package com.xj.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientTest {

    private static final String NEW_TOPIC = "test2";
    private static final String brokerUrl = "192.168.19.136:9092,192.168.19.137:9092,192.168.19.138:9092";

    private static AdminClient adminClient;




    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        //sasl认证
//        properties.put("security.protocol","SASL_PLAINTEXT");
//        properties.put("sasl.mechanism","PLAIN");
//        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='alice' password='alice';");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        adminClient = AdminClient.create(properties);

        try {
//            listTopicsIncludeInternal();
            listConsumerGroupOffsets("xj-0");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public void createTopics(){
        NewTopic newTopic = new NewTopic(NEW_TOPIC,3, (short) 1);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        CreateTopicsResult topics = adminClient.createTopics(newTopicList);
        Map<String, KafkaFuture<Void>> values = topics.values();
        KafkaFuture<Void> test1 = values.get(NEW_TOPIC);
        try {
            Void aVoid = test1.get();
            System.out.println(aVoid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(values);
        System.out.println(topics);
    }



    public static void listConsumerGroupOffsets(String groupId) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> mapKafkaFuture = result.partitionsToOffsetAndMetadata();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mapKafkaFuture.get();
        for (Map.Entry<TopicPartition,OffsetAndMetadata> entry: topicPartitionOffsetAndMetadataMap.entrySet()){
            TopicPartition key = entry.getKey();
            OffsetAndMetadata value = entry.getValue();
            System.out.println(key+" ---> "+value);
        }
    }

    public static void listTopicsIncludeInternal() throws ExecutionException, InterruptedException {
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
        Collection<TopicListing> list = result.listings().get();
        System.out.println(list);
    }


}
