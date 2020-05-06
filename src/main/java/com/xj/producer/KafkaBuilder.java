package com.xj.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class KafkaBuilder<T,K,V> {
    protected static final String SERVERS = "";
    protected static final String DEFAULT_SERRIALIZER = "";
    protected static final String DEFAULT_DESERIALIZER = "";
    protected static final int DEFAULT_BUFFER_SIZE = 33554432;
    protected static final int DEFAULT_BATCH_SIZE = 16384;
    protected static final String DEFAULT_GROUP_ID = "xj_0";

    public Producer<T,K> buildProducer(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers",servers);
        props.put("batch.size", DEFAULT_BATCH_SIZE);
        props.put("linger.ms", 10);
        props.put("buffer.memory", DEFAULT_BUFFER_SIZE);
        props.put("key.serializer", DEFAULT_SERRIALIZER);
        props.put("value.serializer", DEFAULT_DESERIALIZER);
        props.put("max.request.size", 2073741824);
        return new KafkaProducer<T, K>(props);
    }

    public Consumer<K,V> builderConsumer(String servers, String groupId){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id",groupId);
        props.put("enable.auto.commit", true);
        props.put("auto.offset.reset","earliest");
        props.put("max.partition.fetch.bytes",15252880);
        props.put("key.deserializer",DEFAULT_DESERIALIZER);
        props.put("value.deserializer",DEFAULT_DESERIALIZER);
        return new KafkaConsumer<K, V>(props);
    }

}
