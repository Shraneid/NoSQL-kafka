package com.gboissinot.esilv.streaming.data.velib.collection.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gboissinot.esilv.streaming.data.velib.BlockClass;
import com.gboissinot.esilv.streaming.data.velib.BlockCypher;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;

/**
 * @author Gregory Boissinot
 */
class Collector {
    private static final String BLOCKCYPHER_MAIN_URL = "https://api.blockcypher.com/v1/eth/main?token=8a5a107b5d76463295d4231d1ae154b2";
    private static final String BLOCKCYPHER_BLOCK_URL = "https://api.blockcypher.com/v1/eth/main/blocks/";

    private static final Logger logger = LoggerFactory.getLogger(Collector.class);

    private static Integer blockHeight = null;

    private final KafkaPublisher publisher;

    Collector(KafkaPublisher publisher, KafkaPublisher publisherCompact) {
        this.publisher = publisher;
    }

    void collect() {
        try {
            TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                    NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslsf)
                            .register("http", new PlainConnectionSocketFactory())
                            .build();

            BasicHttpClientConnectionManager connectionManager =
                    new BasicHttpClientConnectionManager(socketFactoryRegistry);
            CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                    .setConnectionManager(connectionManager).build();

            HttpComponentsClientHttpRequestFactory requestFactory =
                    new HttpComponentsClientHttpRequestFactory(httpClient);

            //get the main chain state (get height)
            ResponseEntity<String> response =
                    new RestTemplate(requestFactory).exchange(BLOCKCYPHER_MAIN_URL, HttpMethod.GET, null, String.class);
            String jsonString = response.getBody();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(jsonString);

            int currentHeight = jsonNode.get("height").asInt();


            BlockCypher blockCypherObject = BlockCypher.newBuilder()
                    .setName(jsonNode.get("name").asText())
                    .setHeight(jsonNode.get("height").asInt())
                    .setHash(jsonNode.get("hash").asText())
                    .setTime(jsonNode.get("time").asText())
                    .setLatestUrl(jsonNode.get("latest_url").asText())
                    .setPreviousHash(jsonNode.get("previous_hash").asText())
                    .setPreviousUrl(jsonNode.get("previous_url").asText())
                    .setPeerCount(jsonNode.get("peer_count").asInt())
                    .setUnconfirmedCount(jsonNode.get("unconfirmed_count").asInt())
                    .setHighGasPrice(jsonNode.get("high_gas_price").asLong())
                    .setMediumGasPrice(jsonNode.get("medium_gas_price").asLong())
                    .setLowGasPrice(jsonNode.get("low_gas_price").asLong())
                    .setLastForkHeight(jsonNode.get("last_fork_height").asInt())
                    .setLastForkHash(jsonNode.get("last_fork_hash").asText())
                    .build();
            publisher.publish(publisher.topicNameMain, "main", blockCypherObject);


            if (Collector.blockHeight == null){
                Collector.blockHeight = currentHeight;
                response =
                        new RestTemplate(requestFactory).exchange(BLOCKCYPHER_BLOCK_URL + currentHeight, HttpMethod.GET, null, String.class);
                CreateBlockObjectAndPublish(mapper, response.getBody());
            } else {
                // we get intermediary blocks and we push them
                int oldHeight = Collector.blockHeight;
                Collector.blockHeight = currentHeight;
                for (int i = 1; i <= currentHeight - oldHeight - 1; i++) {
                    Thread.sleep(5000);
                    System.out.println("calling : " + BLOCKCYPHER_BLOCK_URL + String.valueOf(Collector.blockHeight+i));
                    try {
                        response =
                                new RestTemplate(requestFactory).exchange(BLOCKCYPHER_BLOCK_URL + String.valueOf(Collector.blockHeight + i) + "?token=8a5a107b5d76463295d4231d1ae154b2", HttpMethod.GET, null, String.class);
                        CreateBlockObjectAndPublish(mapper, response.getBody());
                    } catch (Exception e){
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void CreateBlockObjectAndPublish(ObjectMapper mapper, String jsonString) throws JsonProcessingException {
        JsonNode jsonNode = mapper.readTree(jsonString);

        BlockClass blockCypherObject = BlockClass.newBuilder()
                .setHash(jsonNode.get("hash").asText())
                .setHeight(jsonNode.get("height").asInt())
                .setChain(jsonNode.get("chain").asText())
                .setTotal(jsonNode.get("total").asDouble())
                .setFees(jsonNode.get("fees").asLong())
                .setSize(jsonNode.get("size").asInt())
                .setVer((float)jsonNode.get("ver").asDouble())
                .setTime(jsonNode.get("time").asText())
                .setReceivedTime(jsonNode.get("received_time").asText())
                .setCoinbaseAddr(jsonNode.get("coinbase_addr").asText())
                .setRelayedBy(jsonNode.get("relayed_by").asText())
                .setNonce(jsonNode.get("nonce").asDouble())
                .setNTx(jsonNode.get("n_tx").asInt())
                .setPrevBlock(jsonNode.get("prev_block").asText())
                .setMrklRoot(jsonNode.get("mrkl_root").asText())
                .setTxids(mapper.convertValue(jsonNode.get("txids"), ArrayList.class))
                .setInternalTxids(mapper.convertValue(jsonNode.get("internal_txids"), ArrayList.class))
                .setDepth(jsonNode.get("depth").asInt())
                .setPrevBlockUrl(jsonNode.get("prev_block_url").asText())
                .setTxUrl(jsonNode.get("tx_url").asText())
                .setNextTxids(jsonNode.get("next_txids").asText())
                .build();

//        System.out.println("\n\n_____________________________________\nAdding block n°" + blockCypherObject.getHeight());

        publisher.publish(publisher.topicNameBlock, String.valueOf(blockCypherObject.getHeight()), blockCypherObject);
    }
}