package com.xj.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerService {
    private static Producer<String, String> producer;
    static final String server = "192.168.138.131:9092";


    public static void main(String[] args) {
        KafkaBuilder b = new KafkaBuilder<String, JSONObject, JSONObject>();
        Producer producer = b.buildProducer(server);
        JSONObject jo = new JSONObject();
        for (int i = 0;;i++){
            jo.clear();
            jo.put("a",i);

            producer.send(new ProducerRecord("test0",i+"",jo));
            System.out.println("发送数据："+i);
        }

    }

}
