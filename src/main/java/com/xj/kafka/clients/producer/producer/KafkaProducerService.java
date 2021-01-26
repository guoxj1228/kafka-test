package com.xj.kafka.clients.producer.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class KafkaProducerService {
    private static Producer<String, String> producer;
    static final String server = "192.168.19.136:9092,192.168.19.137:9092,192.168.19.138:9092";


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaBuilder b = new KafkaBuilder<String, JSONObject, JSONObject>();
        Producer producer = b.buildProducer(server);
        JSONObject jo = new JSONObject();
        for (int i = 0;;i++){
            jo.clear();
            jo.put("a",i);

            producer.send(new ProducerRecord("test0",i+"",jo));//.get();
            System.out.println("发送数据："+i);

        }
    }

//    public static class ProducerTest implements Runnable{
//
//        @Override
//        public void run() {
//            KafkaBuilder b = new KafkaBuilder<String, JSONObject, JSONObject>();
//            Producer producer = b.buildProducer(server);
//            JSONObject jo = new JSONObject();
//            for (int i = 0;;i++){
//                jo.clear();
//                jo.put("a",i);
//
//                try {
//                    producer.send(new ProducerRecord("test0",i+"",jo)).get();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("发送数据："+i);
//
//            }
//        }
//    }

}
