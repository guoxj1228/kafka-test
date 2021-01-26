package com.xj.kafka.clients.producer.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class KafkaBuilder<T,K,V> {
    protected static final String SERVERS = "";
    protected static final String KEY_DEFAULT_SERRIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    protected static final String KEY_DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
       protected static final String DEFAULT_SERRIALIZER = "com.xj.kafka.clients.producer.serialize.JsonSerialize";
    protected static final String DEFAULT_DESERIALIZER = "com.xj.kafka.clients.producer.serialize.JsonDeserialize";
    protected static final int DEFAULT_BUFFER_SIZE = 33554432;
    protected static final int DEFAULT_BATCH_SIZE = 1;
    protected static final String DEFAULT_GROUP_ID = "xj_0";

    public Producer<T,K> buildProducer(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers",servers);
        props.put("batch.size", DEFAULT_BATCH_SIZE);
        props.put("linger.ms", 0);
        props.put("buffer.memory", DEFAULT_BUFFER_SIZE);
        props.put("key.serializer", KEY_DEFAULT_SERRIALIZER);
        props.put("value.serializer", DEFAULT_SERRIALIZER);
        props.put("max.request.size", 10485760);
        return new KafkaProducer<T, K>(props);
    }

    public Consumer<T,V> builderConsumer(String servers, String groupId){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id",groupId);
        props.put("enable.auto.commit", true);
        props.put("auto.offset.reset","earliest");
        props.put("max.partition.fetch.bytes",15252880);
        props.put("key.deserializer",KEY_DEFAULT_DESERIALIZER);
        props.put("value.deserializer",DEFAULT_DESERIALIZER);
        return new KafkaConsumer<T, V>(props);
    }

}
