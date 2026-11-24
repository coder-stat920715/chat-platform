# Scalable Chat Platform (Spring Boot 3 / WebFlux / Redis / Kafka / Cassandra)

Reference implementation of the HLD/LLD design: a reactive WebSocket gateway, Kafka as the write-ahead log,
Redis session registry + Pub/Sub routing, Cassandra time-bucketed storage, and push fallback with retry + DLT.

## Modules

| Module | Port | Role |
|---|---|---|
| `chat-common` | - | Domain records, Snowflake ID generator, Kafka/Redis helpers |
| `websocket-gateway` | 8080 | WebFlux WebSocket engine, heartbeats, session registry, ingress (dedupe + Snowflake + Kafka), channel subscriptions, rate limiting |
| `message-router` | 8081 | Consumes `chat.messages.v1` / `chat.events.v1`, resolves recipients, routes via Redis Pub/Sub, offline inbox + push enqueue |
| `message-persister` | 8082 | Batch-persists messages to Cassandra, stores read cursors, serves paginated history API |
| `push-notification-service` | - | Consumes `push.notifications.v1` with `@RetryableTopic` exponential backoff + DLT |

## Message path

```
Client A -> WS Gateway (validate, dedupe, Snowflake id) -> Kafka chat.messages.v1 (acks=all) -> SENT ack to A
   Kafka -> message-persister -> Cassandra
   Kafka -> message-router -> Redis registry lookup -> Redis Pub/Sub gw:{id} -> Gateway B -> Client B
                           \-> offline: Cassandra user_inbox + push.notifications.v1 -> push-notification-service
```

## Run locally

Requirements: JDK 17+, Maven 3.9+, Docker.

```bash
docker compose up -d                 # Kafka, Redis, Cassandra (+ schema init; wait ~1 min on first start)
mvn clean package                    # builds all modules, runs Snowflake unit tests

# one terminal each
java -jar websocket-gateway/target/websocket-gateway-1.0.0.jar
java -jar message-router/target/message-router-1.0.0.jar
java -jar message-persister/target/message-persister-1.0.0.jar
java -jar push-notification-service/target/push-notification-service-1.0.0.jar

./scripts/demo.sh                    # creates conversations and prints websocat commands
```

Run a second gateway: `SERVER_PORT=8090 CHAT_GATEWAY_ID=gw-2 CHAT_WORKER_ID=2 java -jar websocket-gateway/target/*.jar`
and connect users to different gateways to see cross-node routing.

Exercise retry + DLT: `PUSH_FAIL_RATE=0.7 java -jar push-notification-service/target/*.jar`, then send a message to an offline user.

## Wire protocol (JSON text frames)

```json
{"type":"SEND","correlationId":"c1","payload":{"clientMsgId":"<uuid>","conversationId":100,"type":"TEXT","body":"hi"}}
{"type":"RECEIPT","payload":{"conversationId":100,"messageId":123,"status":"DELIVERED"}}   // or READ
{"type":"TYPING","payload":{"conversationId":100,"typing":true}}
{"type":"SUBSCRIBE","payload":{"conversationId":200}}                                       // channel mode
{"type":"PING"}
```
Server frames: `ACK`, `MESSAGE`, `RECEIPT`, `TYPING`, `PONG`, `ERROR`.
Connect: `ws://host:8080/ws/v1/chat?token=dev-{userId}&deviceId={device}`.

## Dev shortcuts to replace before production

- `DevAuthWebFilter`: accepts `token=dev-{userId}`. Replace with short-lived device-bound JWT validation.
- `ConversationDirectory` (Redis sets) stands in for `group-chat-service` + PostgreSQL membership.
- `DevController` (create conversation, presence) and the unauthenticated history endpoint.
- `LoggingPushGateway` stands in for FCM/APNs.
- Single-node Kafka/Cassandra settings (RF=1) in `docker-compose.yml` / `schema.cql`. Production: RF=3, `min.insync.replicas=2`, `LOCAL_QUORUM`.
- Poison messages are logged and skipped (see `KafkaPipelines`); add a DLQ topic for them.
- Snowflake `worker-id` and `gateway-id` must be unique per instance (StatefulSet ordinal or Redis lease).

## Not included (see the design doc)

Media service (S3 pre-signed URLs), search indexer (OpenSearch), auth-service, API gateway, multi-region replication,
Redis read-after-write overlay, Prometheus dashboards.
