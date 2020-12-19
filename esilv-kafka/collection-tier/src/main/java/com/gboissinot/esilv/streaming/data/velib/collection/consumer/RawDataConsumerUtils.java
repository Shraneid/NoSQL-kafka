package com.gboissinot.esilv.streaming.data.velib.collection.consumer;

import com.gboissinot.esilv.streaming.data.velib.BlockClass;
import com.gboissinot.esilv.streaming.data.velib.BlockCypher;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.gboissinot.esilv.streaming.data.velib.config.KafkaConfig.*;

/**
 * @author Gregory Boissinot
 */
public class RawDataConsumerUtils {

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-test-1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        final Consumer<String, BlockClass> blocksConsumer = new KafkaConsumer<>(properties);
        blocksConsumer.subscribe(Collections.singletonList(RAW_TOPIC_NAME_BLOCK));

        final Consumer<String, BlockCypher> mainConsumer = new KafkaConsumer<>(properties);
        mainConsumer.subscribe(Collections.singletonList(RAW_TOPIC_NAME_MAIN));

        //blocks consumer
        ConsumerRecords<String, BlockClass> records;
        ConsumerRecords<String, BlockCypher> mainRecords;
        try{
//            while (true){
            double totalFees;
            long totalNtxs;
            for (int i = 0; i < 5; i++){
                totalFees = 0;
                totalNtxs = 0;

                blocksConsumer.seekToBeginning(blocksConsumer.assignment());
                records = blocksConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, BlockClass> record : records){
                    System.out.println(record.value());

                    BlockClass o = record.value();

                    System.out.println(o.getHeight());

                    totalFees += o.getFees();
                    totalNtxs += o.getNTx();
                }

                if (records.count() > 0) {
                    System.out.println("Use Case #1 : Get the average fees and convert to ETH");
                    System.out.println("avg fees : " + totalFees / records.count() + ", avg fees in ETH : "
                            + ((float) (totalFees / records.count()) / 100000000.0f));

                    System.out.println("Use Case #2 : Get the average number of transactions");
                    System.out.println("avg tx nb : " + totalNtxs / records.count());
                }
            }
        } finally {
            blocksConsumer.close();
        }

        //main consumer
        try{
            while (true){
                mainConsumer.seekToBeginning(mainConsumer.assignment());
                mainRecords = mainConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, BlockCypher> record : mainRecords){
                    System.out.println(record.value());

                    BlockCypher o = record.value();

                    if (o.getName().toString().equals("ETH.main")){
                   }
                }
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            mainConsumer.close();
        }
    }
}
