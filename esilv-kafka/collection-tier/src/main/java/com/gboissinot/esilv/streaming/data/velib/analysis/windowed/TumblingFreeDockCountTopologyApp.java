package com.gboissinot.esilv.streaming.data.velib.analysis.windowed;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.BOOTSTRAP_SERVERS;

public class TumblingFreeDockCountTopologyApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "velibstats-application-window-1");
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        TumblingFreeDockCountTopologyApp dockCountApp = new TumblingFreeDockCountTopologyApp();

        KafkaStreams streams = new KafkaStreams(dockCountApp.createTopology(), config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            streams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private Topology createTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Long> stream =
                builder
                        .stream("velib-nbfreedocks-updates", Consumed.with(Serdes.String(), Serdes.Long()))
                        .map((key, value) -> new KeyValue<>("Count", value));

        KGroupedStream<String, Long> groupedStream =
                stream.groupByKey();

        KTable<Windowed<String>, Long> timeWindowedAggregatedStream =
                groupedStream
                        .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))

                        .aggregate(
                                () -> 0L,
                                (aggKey, newValue, aggValue) -> aggValue + newValue,
                                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                                        .withValueSerde(Serdes.Long()));

        timeWindowedAggregatedStream.toStream().to("velib-nbfreedocks-count-notifications", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));

        return builder.build();
    }
}
