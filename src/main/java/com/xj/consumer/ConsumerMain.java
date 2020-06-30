package com.xj.consumer;

public class ConsumerMain {
    private static int consumerNum = 3;
    private static String groupId = "xj-g";
    private static String topic = "test0";
    private static String brokerList = "192.168.19.136:9092,192.168.19.137:9092,192.168.19.138:9092";

    public static void main(String[] args) throws InterruptedException {
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.submit(new ConsumerTask() {
            @Override
            public void doTask(String msg) {
                System.out.println(msg);
            }
        });

        Thread.sleep(5000);

        consumerGroup.shutDown();
    }
}
