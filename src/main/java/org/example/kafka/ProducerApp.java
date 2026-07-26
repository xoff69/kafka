package org.example.kafka;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class ProducerApp {

    private static final String TOPIC = "poc-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int PORT = 8091;

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/sendMessage", exchange -> handleSendMessage(exchange, producer));
        server.setExecutor(null);
        server.start();
        System.out.println("Producer REST demarre sur http://localhost:" + PORT
                + " (POST /sendMessage) -> topic '" + TOPIC + "'");
    }

    private static void handleSendMessage(HttpExchange exchange, KafkaProducer<String, String> producer) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, new JSONObject().put("error", "Methode non autorisee").toString());
            return;
        }
        try {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String message = new JSONObject(body).getString("message");

            producer.send(new ProducerRecord<>(TOPIC, message), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Erreur d'envoi: " + exception.getMessage());
                } else {
                    System.out.printf("Envoye -> partition %d, offset %d%n", metadata.partition(), metadata.offset());
                }
            });

            JSONObject response = new JSONObject().put("status", "accepted").put("message", message);
            sendResponse(exchange, 202, response.toString());
        } catch (Exception e) {
            sendResponse(exchange, 400, new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private static void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
