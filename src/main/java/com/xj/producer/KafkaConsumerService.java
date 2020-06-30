package com.xj.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;


public class KafkaConsumerService {
    static final String server = "192.168.19.136:9092,192.168.19.137:9092,192.168.19.138:9092";
    static final String groupId = "xj-0";
    public static void main(String[] args) {
        KafkaBuilder<String, JSONObject, JSONObject> builder = new KafkaBuilder<>();
        Consumer<String, JSONObject> consumer = builder.builderConsumer(server, groupId);
        consumer.subscribe(Arrays.asList("test0"));
        while(true){
            ConsumerRecords<String, JSONObject> records = consumer.poll(Duration.ofMinutes(100));
            for (ConsumerRecord<String, JSONObject> record: records){

                JSONObject value = record.value();
                System.out.println(record.key()+", "+value);
                System.out.println(value.toJSONString());
            }
        }

    }
}
