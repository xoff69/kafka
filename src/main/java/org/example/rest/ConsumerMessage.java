package org.example.rest;

public record ConsumerMessage(long id, String message, String createdAt) {
}
