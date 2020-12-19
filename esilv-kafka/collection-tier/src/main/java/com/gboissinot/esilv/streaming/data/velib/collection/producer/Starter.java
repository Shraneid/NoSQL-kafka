package com.gboissinot.esilv.streaming.data.velib.collection.producer;

/**
 * @author Gregory Boissinot
 */
public class Starter {

    public static void main(String[] args) throws InterruptedException {
        KafkaPublisher publisher = new KafkaPublisher();
        KafkaPublisher publisher2 = new KafkaPublisher();
        Collector collector = new Collector(publisher, publisher2);
        scheduleCollect(collector);
    }

    private static void scheduleCollect(Collector collector) throws InterruptedException {
        ScheduledExecutorRepeat executorRepeat = new ScheduledExecutorRepeat(collector, 3);
        executorRepeat.repeat();
    }
}
