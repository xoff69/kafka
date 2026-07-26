# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A command-line Kafka producer/consumer POC. A single-node Kafka broker (KRaft mode, no Zookeeper) runs
in Docker; two standalone Java apps connect to it from the host to send and receive messages on the
topic `poc-topic`. The producer exposes a REST endpoint (`POST /sendMessage`) instead of reading stdin.
The consumer also forwards every record to a small REST service, which persists it in SQLite.

## Commands

Use the Gradle wrapper (`gradlew.bat` on Windows / `./gradlew` in bash) — do not rely on a global `gradle` install.

```
docker compose up -d             # start the Kafka broker (localhost:9092)
docker compose down              # stop and remove it

gradlew.bat restApi              # REST service: POST/GET /consumer on localhost:8090, backed by SQLite
gradlew.bat producer              # producer REST service: POST /sendMessage on localhost:8091 sends to 'poc-topic'
gradlew.bat consumer             # consumer: polls 'poc-topic', prints every record, POSTs it to the REST service

gradlew.bat build                # compile + run tests
gradlew.bat test                 # run all tests (JUnit 5 / Jupiter, via junit-platform-launcher)
gradlew.bat test --tests "org.example.SomeTest"              # single test class
gradlew.bat test --tests "org.example.SomeTest.someMethod"   # single test method
```

The broker must be up (`docker compose up -d`) and the REST service must be running (`gradlew.bat
restApi`) before running the consumer task — it connects to `localhost:9092` (Kafka) and
`localhost:8090` (REST) with no retry/backoff beyond each client's own defaults.

With `gradlew.bat producer` running, send a message to `poc-topic` via `POST /sendMessage`:

```
# PowerShell (curl.exe explicitly — the `curl` alias is Invoke-WebRequest and handles quoting differently)
curl.exe -X POST http://localhost:8091/sendMessage -H "Content-Type: application/json" -d '{\"message\":\"hello\"}'

# PowerShell native
Invoke-RestMethod -Uri http://localhost:8091/sendMessage -Method Post -ContentType "application/json" -Body '{"message":"hello"}'

# bash
curl -X POST http://localhost:8091/sendMessage -H "Content-Type: application/json" -d '{"message":"hello"}'
```

Expect `202 Accepted` with `{"status":"accepted","message":"hello"}`.

## Architecture

- `docker-compose.yml` — single `broker` service (`apache/kafka:3.8.0`), combined broker+controller KRaft
  roles, dual listeners: `PLAINTEXT` on the internal Docker network (`broker:19092`, for future containers)
  and `PLAINTEXT_HOST` on `localhost:9092` (mapped for the host-side Java apps). This is the standard
  single-node KRaft layout from the upstream Apache Kafka Docker examples — if you add more services to
  the compose network, connect them via `broker:19092`, not `localhost:9092`.
- `src/main/java/org/example/kafka/ProducerApp.java` — JDK built-in `com.sun.net.httpserver.HttpServer`
  on port `8091`, single context `/sendMessage`: `POST` with body `{"message": "..."}` sends the message
  as a `ProducerRecord` to `poc-topic` and returns `202 Accepted` immediately (the send callback logs the
  resulting partition/offset or any error asynchronously, it does not block the HTTP response).
- `src/main/java/org/example/kafka/ConsumerApp.java` — subscribes to `poc-topic` under consumer group
  `poc-consumer-group`, polls in a `while(true)` loop (500ms), prints partition/offset/value for each
  record, then POSTs `{"message": <value>}` to `http://localhost:8090/consumer` (errors are logged to
  stderr, not fatal — the poll loop keeps going). `auto.offset.reset=earliest`, so a fresh run replays
  the whole topic (and re-POSTs every record).
- `src/main/java/org/example/rest/RestServerApp.java` — JDK built-in `com.sun.net.httpserver.HttpServer`
  on port `8090`, single context `/consumer`: `POST` inserts into SQLite and returns the created row,
  `GET` returns all rows as a JSON array. No framework (no Spring) — kept deliberately minimal for a POC.
- `src/main/java/org/example/rest/ConsumerRepository.java` — JDBC access to the SQLite file
  `data/consumer.db` (created on first run, gitignored); creates table `t_consumer(id, message,
  created_at)` if missing.
- `src/main/java/org/example/rest/ConsumerMessage.java` — record representing one row of `t_consumer`.
- `build.gradle.kts` — `application` plugin is applied but `mainClass` just defaults to `ProducerApp`;
  the real entry points are the custom `producer`, `consumer`, and `restApi` `JavaExec` tasks (each
  pinning its own `mainClass`). Kafka client version (`kafka-clients:3.8.0`) is pinned to match
  the broker image tag — bump both together if you upgrade. `sqlite-jdbc` and `org.json:json` are used
  for the REST service (JDBC driver + minimal JSON, no Spring/Jackson).
- Topic (`poc-topic`) is auto-created on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`); there is no
  topic-management/admin code in this POC.

## Not yet present

No integration tests exercise the Kafka flow or the REST service (the JUnit setup is template-only,
`src/test` does not exist). No schema registry / Avro / Protobuf — messages are plain `String` key/value.
No security (SASL/TLS on Kafka, auth on the REST service) — PLAINTEXT/unauthenticated only, suitable for
local POC use only.
