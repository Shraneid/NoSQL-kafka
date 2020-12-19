package com.gboissinot.esilv.streaming.data.velib.collection.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.*;

/**
 * @author Gregory Boissinot
 */
class KafkaPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    public final String topicNameBlock;
    public final String topicNameMain;
    private final Producer<String, Object> producer;

    KafkaPublisher() {
        this.producer = createProducer();
        this.topicNameBlock = RAW_TOPIC_NAME_BLOCK;
        this.topicNameMain = RAW_TOPIC_NAME_MAIN;
        registerShutdownHook(producer);
    }

    private Producer<String, Object> createProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(Boolean.TRUE));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaProducer<>(properties);
    }

    void publish(String tn, String key, Object o) {
        logger.info(String.format("Publishing %s", o));
        ProducerRecord<String, Object> record = new ProducerRecord<>(tn, key, o);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                logger.error(e.getMessage(), e);
            } else {
                logger.info("Batch record sent.");
            }
        });
    }

    private void registerShutdownHook(final Producer<String, Object> producer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.debug("Closing Kafka publisher ...");
            producer.close(Duration.ofMillis(2000));
            logger.info("Kafka publisher closed.");
        }));
    }
}
