package com.xj.kafka.clients.producer.admin;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * kafka监控 topic的数据消费情况
 *
 * @author LiXuekai on 2020/9/16
 */
public class KafkaMonitorTools {}/*{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMonitorTools.class);


    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOGGER.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    *//**
     * @param brokers broker 地址
     * @param topic   topic
     * @return map<分区, 分区count信息>
     *//*
    public static Map<Integer, PartitionMetadata> findLeader(List<String> brokers, String topic) {
        Map<Integer, PartitionMetadata> map = Maps.newHashMap();
        for (String broker : brokers) {
            SimpleConsumer consumer = null;
            try {
                String[] hostAndPort = broker.split(":");
                consumer = new SimpleConsumer(hostAndPort[0], Integer.parseInt(hostAndPort[1]), 100000, 64 * 1024, "leaderLookup" + new Date().getTime());
                List<String> topics = Lists.newArrayList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        map.put(part.partitionId(), part);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error communicating with Broker [" + broker + "] to find Leader for [" + topic + ", ] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        return map;
    }

    public static Map<String, Long> monitor(List<String> brokers, List<String> topics) {
        if (brokers == null || brokers.isEmpty()) {
            return null;
        }
        if (topics == null || topics.isEmpty()) {
            return null;
        }
        Map<String, Long> map = Maps.newTreeMap();
        for (String topicName : topics) {
            Map<Integer, PartitionMetadata> metadata = findLeader(brokers, topicName);
            long size = 0L;
            for (Map.Entry<Integer, PartitionMetadata> entry : metadata.entrySet()) {
                int partition = entry.getKey();
                String leadBroker = entry.getValue().leader().host();
                String clientName = "Client_" + topicName + "_" + partition;
                SimpleConsumer consumer = new SimpleConsumer(leadBroker, entry.getValue().leader().port(), 100000, 64 * 1024, clientName);
                long readOffset = getLastOffset(consumer, topicName, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                size += readOffset;
                consumer.close();
            }
            map.put(topicName, size);
        }
        return map;
    }
}*/
