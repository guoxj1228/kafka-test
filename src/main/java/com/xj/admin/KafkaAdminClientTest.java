package com.xj.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientTest {

    private static final String NEW_TOPIC = "test2";
    private static final String brokerUrl = "192.168.19.136:9092,192.168.19.137:9092,192.168.19.138:9092";

    private static AdminClient adminClient;


    private static void initialize(){
        Properties properties = new Properties();
        //sasl认证
//        properties.put("security.protocol","SASL_PLAINTEXT");
//        properties.put("sasl.mechanism","PLAIN");
//        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='alice' password='alice';");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        adminClient = AdminClient.create(properties);
    }


    public static void main(String[] args) throws IOException {

        initialize();

        try {
//            listTopicsIncludeInternal();
//            listConsumerGroupOffsets("xj-0");
            ArrayList<Integer> integers = new ArrayList<>();
            integers.add(1);
//            integers.add(2);
//            integers.add(3);
//            describeLogDirs(integers);
            long test0 = getTopicDiskSizeForSomeBroker("test0", integers);
            System.out.println(test0);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            shutdown();
        }


    }

    public static long getTopicDiskSizeForSomeBroker(String topic, Collection<Integer> brokerID)
            throws ExecutionException, InterruptedException {
        long sum = 0;
        DescribeLogDirsResult ret = adminClient.describeLogDirs(brokerID);
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> tmp = ret.all().get();
        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> entry : tmp.entrySet()) {
            Map<String, DescribeLogDirsResponse.LogDirInfo> tmp1 = entry.getValue();
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> entry1 : tmp1.entrySet()) {
                DescribeLogDirsResponse.LogDirInfo info = entry1.getValue();
                Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoMap = info.replicaInfos;
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : replicaInfoMap.entrySet()) {
                    if (topic.equals(replicas.getKey().topic())) {
                        sum += replicas.getValue().size;
                    }
                }
            }
        }
        return sum;
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

    public static void describeLogDirs(Collection<Integer> ids) throws ExecutionException, InterruptedException {

        DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(ids);
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> integerMapMap = describeLogDirsResult.all().get();
        for(Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> entry: integerMapMap.entrySet()){
            Integer key = entry.getKey();
            Map<String, DescribeLogDirsResponse.LogDirInfo> value = entry.getValue();
            System.out.println(key+" ---> "+ value);
        }
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

    public static void createAcl() throws ExecutionException, InterruptedException {

        List<AclBinding> aclBindings = new ArrayList<>();

        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "test0", PatternType.LITERAL);
        AccessControlEntry entry = new AccessControlEntry("User:writer", "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(pattern, entry);


        ResourcePattern pattern1 = new ResourcePattern(ResourceType.TOPIC, "test0", PatternType.LITERAL);
        AccessControlEntry entry1 = new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AclBinding aclBinding1 = new AclBinding(pattern, entry);

        ResourcePattern pattern2 = new ResourcePattern(ResourceType.GROUP, "xj-0", PatternType.LITERAL);
        AccessControlEntry entry2 = new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AclBinding aclBinding2 = new AclBinding(pattern, entry);

        aclBindings.add(aclBinding1);
        aclBindings.add(aclBinding2);
        aclBindings.add(aclBinding);

        CreateAclsResult acls = adminClient.createAcls(aclBindings);
        Void aVoid = acls.all().get();


    }

    public static void listTopicsIncludeInternal() throws ExecutionException, InterruptedException {
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
        Collection<TopicListing> list = result.listings().get();
        System.out.println(list);
    }

    private static void shutdown() {
        if (adminClient != null) {
            adminClient.close();
        }
    }


}
