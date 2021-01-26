package com.xj.kafka.clients.producer.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientTest {

    private static final String NEW_TOPIC = "test0";
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
//            listConsumerGroupOffsets("xj0");
//            ArrayList<Integer> integers = new ArrayList<>();
//            integers.add(1);
//            integers.add(2);
//            integers.add(3);
//            describeLogDirs(integers);
//            long test0 = getTopicDiskSizeForSomeBroker("test0", integers);
//            System.out.println(test0);

            Long dataCount = getDataCount("test0", "xj0", "2021-01-22 08:21:00", "2021-01-22 11:21:00");
            System.out.println(dataCount);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            shutdown();
        }



    }


    public static KafkaConsumer<String, String> getConsumer(String groupId){
        Properties props = new Properties();
        props.put("bootstrap.servers", "node4:9092");
        props.put("group.id",groupId);
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("linger.ms", 10);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //sasl
//        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='reader' password='reader';");
//        props.setProperty("security.protocol", "SASL_PLAINTEXT");
//        props.setProperty("sasl.mechanism", "PLAIN");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public static  Map<TopicPartition, Long> getSerchTime(String topic,String groupId, String searchTime){

        KafkaConsumer<String, String> consumer = getConsumer(groupId);

        // 获取topic的partition信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
//            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            Date now = new Date();
//            long nowTime = now.getTime();
//            System.out.println("当前时间: " + df.format(now));
//            long fetchDataTime = nowTime - 1000 * 60 * 30;  // 计算30分钟之前的时间戳


        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startParse = LocalDateTime.parse(searchTime, df);
        long fetchStratDataTime = LocalDateTime.from(startParse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();


        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchStratDataTime);
        }

        consumer.assign(topicPartitions);
        return timestampsToSearch;
    }

    public static Long getDataCount(String topic, String groupId, String startTime, String endTime){

        long dataCount = 0L;
        try {

            KafkaConsumer<String, String> consumer = getConsumer(groupId);
            Map<TopicPartition, OffsetAndTimestamp> startMap = consumer.offsetsForTimes(getSerchTime(topic,groupId,startTime));
            Map<TopicPartition, OffsetAndTimestamp> endMap = consumer.offsetsForTimes(getSerchTime(topic,groupId,endTime));

            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = null;
            if(endMap != null && endMap.size() > 0 && endMap.values() != null && endMap.values().toArray()[0] == null){
                endMap = null;
                topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsets(groupId);
            }



            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : startMap.entrySet()) {

                OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
                long startOffset = offsetAndTimestamp.offset();
                long endOffset;
                if (endMap == null){
                    OffsetAndMetadata offsetAndMetadata = topicPartitionOffsetAndMetadataMap.get(entry.getKey());
                    endOffset = offsetAndMetadata.offset();
                }else{
                    OffsetAndTimestamp offsetAndTimestamp1 = endMap.get(entry.getKey());
                    endOffset = offsetAndTimestamp1.offset();
                }
                dataCount += endOffset-startOffset;
                System.out.println("key: " + entry.getKey() + " ,value: " + entry.getValue());
            }


//            OffsetAndTimestamp offsetTimestamp = null;
//            System.out.println("开始设置各分区初始偏移量...");
//            for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
//                // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
//                offsetTimestamp = entry.getValue();
//                if(offsetTimestamp != null) {
//                    int partition = entry.getKey().partition();
//                    long timestamp = offsetTimestamp.timestamp();
//                    long offset = offsetTimestamp.offset();
//                    System.out.println("partition = " + partition +
//                            ", time = " + df.format(new Date(timestamp))+
//                            ", offset = " + offset);
//                    // 设置读取消息的偏移量
//                    consumer.seek(entry.getKey(), offset);
//                }
//            }
//            System.out.println("设置各分区初始偏移量结束...");

        }catch (Exception e){
            e.printStackTrace();
        }
        return dataCount;
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


    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> mapKafkaFuture = result.partitionsToOffsetAndMetadata();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mapKafkaFuture.get();
        for (Map.Entry<TopicPartition,OffsetAndMetadata> entry: topicPartitionOffsetAndMetadataMap.entrySet()){
            TopicPartition key = entry.getKey();
            OffsetAndMetadata value = entry.getValue();
            System.out.println(key+" ---> "+value);
        }
        return topicPartitionOffsetAndMetadataMap;
    }

    public static void test(){
//        adminClient.deleteRecords();
//        adminClient.createDelegationToken()
    }

    public static void deleteAcls(){

        ResourcePatternFilter patternFilter = new ResourcePatternFilter(ResourceType.TOPIC, "name", PatternType.LITERAL);
        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter("user:reader", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AclBindingFilter aclBindingFilter = new AclBindingFilter(patternFilter, entryFilter);
        ArrayList<AclBindingFilter> aclBindingFilters = new ArrayList<>();
        aclBindingFilters.add(aclBindingFilter);

        adminClient.deleteAcls(aclBindingFilters);
    }

    public static void createAcl() throws ExecutionException, InterruptedException {

        List<AclBinding> aclBindings = new ArrayList<>();

        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "test0", PatternType.LITERAL);
        AccessControlEntry entry = new AccessControlEntry("User:writer", "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(pattern, entry);


        ResourcePattern pattern1 = new ResourcePattern(ResourceType.TOPIC, "test0", PatternType.LITERAL);
        AccessControlEntry entry1 = new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AclBinding aclBinding1 = new AclBinding(pattern1, entry1);

        ResourcePattern pattern2 = new ResourcePattern(ResourceType.GROUP, "xj-0", PatternType.LITERAL);
        AccessControlEntry entry2 = new AccessControlEntry("User:reader", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AclBinding aclBinding2 = new AclBinding(pattern2, entry2);

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
