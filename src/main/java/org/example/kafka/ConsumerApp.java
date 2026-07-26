package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerApp {

    private static final String TOPIC = "poc-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "poc-consumer-group";
    private static final String REST_ENDPOINT = "http://localhost:8090/consumer";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        HttpClient httpClient = HttpClient.newHttpClient();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            System.out.println("Consumer pret, ecoute '" + TOPIC + "'. Ctrl+C pour arreter.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Recu -> partition %d, offset %d, value=%s%n",
                            record.partition(), record.offset(), record.value());
                    sendToRestApi(httpClient, record.value());
                }
            }
        }
    }

    private static void sendToRestApi(HttpClient httpClient, String message) {
        String body = new JSONObject().put("message", message).toString();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(REST_ENDPOINT))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 201) {
                System.err.println("Echec appel service REST (status " + response.statusCode() + "): " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Erreur appel service REST: " + e.getMessage());
        }
    }
}
