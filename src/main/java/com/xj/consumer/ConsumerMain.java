package com.xj.consumer;

public class ConsumerMain {
    private static int consumerNum = 3;
    private static String groupId = "xj_g";
    private static String topic = "xj_t";
    private static String brokerList = "";

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
