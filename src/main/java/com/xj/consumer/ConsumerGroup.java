package com.xj.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroup {
    private int consumerNum;
    private ExecutorService executor;
    private List<ConsumerConnector> consumerConnectors;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList){
        this.consumerNum = consumerNum;
        consumerConnectors = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++){
            ConsumerConnector consumerConnector = new ConsumerConnector(brokerList, groupId, topic);
            consumerConnectors.add(consumerConnector);
        }
    }

    public void submit(ConsumerTask task){
        this.executor = Executors.newFixedThreadPool(consumerNum);
        submitTasks(this.executor,consumerConnectors, task);
    }

    public static class BusinessTask implements Runnable{
        private ConsumerConnector consumerConnector;
        private KafkaConsumer<String, String> consumer;
        private ConsumerTask task;

        private BusinessTask(ConsumerConnector consumerConnector, ConsumerTask task){
            this.consumerConnector = consumerConnector;
            this.task = task;
            this.consumer = consumerConnector.getConsumer();
        }

        public void run(){
            try {
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record : records){
                        task.doTask(record.value());
                    }
                    consumer.commitSync();
                }
            }catch (Exception e){
                consumer.commitSync();
                consumer.close();
                System.out.println("Close consumer and we are done");
            }
        }
    }

    private void submitTasks(ExecutorService executor, List<ConsumerConnector> consumerConnectors, ConsumerTask task){
        for(ConsumerConnector consumerConnector : consumerConnectors){
            executor.submit(new BusinessTask(consumerConnector,task));
        }
    }

    public void shutDown(){
        if(consumerConnectors != null){
            for(ConsumerConnector consumerConnector: consumerConnectors){
                consumerConnector.shutDown();
            }
        }

        if(executor != null){
            executor.shutdown();
            try{
                if(!executor.awaitTermination(5000, TimeUnit.MICROSECONDS));{
                    System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
