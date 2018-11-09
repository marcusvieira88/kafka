package com.github.marcusvieira.com.github.marcusvieira.tutorial4;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;

public class StreamsFilterTwitter {

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-filter-twitter");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter((String key, String tweet) -> {
            //only show tweets of users with more than 10000 followers
            return extractUserFollowersFromTweet(tweet) > 10000;
        });
        filteredStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamBuilder.build(),
                properties
        );

        //start our streams application
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersFromTweet(String tweetJson) {
        //gson library
        JsonParser jsonParser = new JsonParser();
        JsonElement user = jsonParser.parse(tweetJson).getAsJsonObject().get("user");

        if (user == null) {
            return 0;
        } else {
            JsonElement followersCount = user.getAsJsonObject().get("followers_count");
            if (followersCount == null) {
                return 0;
            } else {
                return followersCount.getAsInt();
            }
        }
    }
}
