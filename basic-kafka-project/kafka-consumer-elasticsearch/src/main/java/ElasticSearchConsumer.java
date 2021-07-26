import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        String hostname = System.getProperty("hostname");
        String username = System.getProperty("username");
        String password = System.getProperty("password");

        RestHighLevelClient client = createClient(hostname, username, password);
        KafkaConsumer kafkaConsumer = createKafkaConsumer("twitter_tweets");

        // Poll for new data
        while(true) {
            // When you poll. you need to give it a timout for how long it should try to get data
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            LOGGER.info("Received: " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record: records) {
                // You have 2 strategies to create an id
                // Option 1: Kafka Generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // Option 2: Use a specific ID already defined in the data you're consuming. Preferred!
                String id = extractIdFromTweet(record.value());

                // This will fail if there's no 'twitter' index created. Make sure to create it in ElasticSearch
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id) // This is to make our consumer idempotent
                        .source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);
            }

            if(recordCount != 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOGGER.info("Committing offsets...");
                kafkaConsumer.commitSync();
                LOGGER.info("Offsets have been committed");

                try {
                    Thread.sleep(1000); // Introducing a small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static RestHighLevelClient createClient(String hostname, String username, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // Create Consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disables autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Max number of records per poll

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Subscribe Consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
}
