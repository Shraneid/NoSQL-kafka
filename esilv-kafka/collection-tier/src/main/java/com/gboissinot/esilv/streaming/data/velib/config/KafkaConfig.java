package com.gboissinot.esilv.streaming.data.velib.config;

import java.util.Collections;
import java.util.List;

public class KafkaConfig {

    public static final List<String> BOOTSTRAP_SERVERS = Collections.singletonList("localhost:9092");

    public static final String RAW_TOPIC_NAME_BLOCK = "blockcypher";
    public static final String RAW_TOPIC_NAME_MAIN = "compact";
}
