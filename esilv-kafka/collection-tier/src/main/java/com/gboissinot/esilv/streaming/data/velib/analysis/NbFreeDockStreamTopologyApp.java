package com.gboissinot.esilv.streaming.data.velib.analysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Properties;

import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.RAW_TOPIC_NAME_BLOCK;

class NbFreeDockStreamTopologyApp {

    static final String STREAM_APP_1_OUT = "velib-nbfreedocks-updates";
    private static final String VELIBSTATS_STREAM_APPLICATION = "velibstats-stream-application";
    private static final String STREAM_APP_1_INPUT = RAW_TOPIC_NAME_BLOCK;

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, VELIBSTATS_STREAM_APPLICATION);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        NbFreeDockStreamTopologyApp dockCountApp = new NbFreeDockStreamTopologyApp();

        KafkaStreams streams = new KafkaStreams(dockCountApp.createTopology(), config);
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

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stats = builder.stream(STREAM_APP_1_INPUT);
        KStream<String, Long> docksCountStream = stats
                .selectKey((key, jsonRecordString) -> extract_station_name(jsonRecordString))
                .map((key, value) -> new KeyValue<>(key, extract_nbfreeedock(value)));

        docksCountStream.to(STREAM_APP_1_OUT, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    private String extract_station_name(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode stationNameNode = fieldsMode.get("name");

        return stationNameNode.asText();
    }

    private Long extract_nbfreeedock(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode nbfreeedockNode = fieldsMode.get("numbikesavailable");

        return Long.parseLong(nbfreeedockNode.asText());
    }
}
