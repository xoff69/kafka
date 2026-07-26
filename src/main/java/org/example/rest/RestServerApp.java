package org.example.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class RestServerApp {

    private static final int PORT = 8090;
    private static final String DB_PATH = "data/consumer.db";

    public static void main(String[] args) throws IOException {
        Path dbFile = Path.of(DB_PATH);
        Files.createDirectories(dbFile.getParent());
        ConsumerRepository repository = new ConsumerRepository("jdbc:sqlite:" + dbFile);

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/consumer", exchange -> handleConsumer(exchange, repository));
        server.setExecutor(null);
        server.start();
        System.out.println("Service REST demarre sur http://localhost:" + PORT
                + " (POST/GET /consumer), base SQLite: " + dbFile.toAbsolutePath());
    }

    private static void handleConsumer(HttpExchange exchange, ConsumerRepository repository) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case "POST" -> handlePost(exchange, repository);
                case "GET" -> handleGet(exchange, repository);
                default -> sendResponse(exchange, 405, new JSONObject().put("error", "Methode non autorisee").toString());
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, new JSONObject().put("error", e.getMessage()).toString());
        }
    }

    private static void handlePost(HttpExchange exchange, ConsumerRepository repository) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        JSONObject json = new JSONObject(body);
        String message = json.getString("message");

        ConsumerMessage saved = repository.insert(message);
        JSONObject response = new JSONObject()
                .put("id", saved.id())
                .put("message", saved.message())
                .put("createdAt", saved.createdAt());
        sendResponse(exchange, 201, response.toString());
    }

    private static void handleGet(HttpExchange exchange, ConsumerRepository repository) throws IOException {
        List<ConsumerMessage> messages = repository.findAll();
        JSONArray array = new JSONArray();
        for (ConsumerMessage m : messages) {
            array.put(new JSONObject()
                    .put("id", m.id())
                    .put("message", m.message())
                    .put("createdAt", m.createdAt()));
        }
        sendResponse(exchange, 200, array.toString());
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
