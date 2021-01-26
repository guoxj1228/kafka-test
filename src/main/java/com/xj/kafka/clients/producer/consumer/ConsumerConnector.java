package com.xj.kafka.clients.producer.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.*;

public class ConsumerConnector<K,V> {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerConnector(String brokerList, String groupId, String topic){
        Properties props = new Properties();
        props.put("bootstrap",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringSerializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public KafkaConsumer<String, String> getConsumer(){
        return consumer;
    }

    public void shutDown(){
        consumer.wakeup();
    }


}
