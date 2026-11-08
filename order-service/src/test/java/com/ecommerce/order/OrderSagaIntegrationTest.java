package com.ecommerce.order;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// The replies of inventory and payment are sent by the test. Debezium is not part of it.
// mock:// is an in-memory Schema Registry, shared by every client in this JVM that uses the same name
@SpringBootTest(properties = {"management.tracing.export.enabled=false", "schema-registry.url=mock://order-test"})
@Testcontainers
class OrderSagaIntegrationTest {

    private static final List<String> REPLY_TOPICS = List.of("inventory", "payments");
    // A copy of inventory's schema, as inventory would write it
    private static final Schema STOCK_RESERVED = new Schema.Parser().parse("""
            {"type": "record", "name": "StockReserved", "namespace": "com.ecommerce.inventory", "fields": [
              {"name": "eventId", "type": {"type": "string", "logicalType": "uuid"}},
              {"name": "occurredAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
              {"name": "orderId", "type": {"type": "string", "logicalType": "uuid"}}]}""");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.1")
            .withInitScript("create-debezium-role.sql");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

    @Autowired
    OrderService orderService;


    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    KafkaAdmin kafkaAdmin;

    // The outbox row is deleted in its own transaction, so a trigger keeps a copy of everything the service sends
    KafkaProducer<String, byte[]> producer;
    // DLT records written by earlier tests, so each test checks only its own
    Map<String, Long> dltBefore = new HashMap<>();
    KafkaAvroSerializer avroSerializer;

    // Bytes, like the real inventory topic: Avro for StockReserved, JSON for the rest
    @BeforeEach
    void createProducer() {
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer());
        avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(Map.of(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://order-test",
                KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName()), false);
    }

    @BeforeEach
    void countDeadLetters() throws Exception {
        for (String topic : REPLY_TOPICS) {
            dltBefore.put(topic, endOffset(topic + "-dlt"));
        }
    }

    @AfterEach
    void closeProducer() {
        producer.close();
    }

    @BeforeEach
    void recordSentMessages() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS sent_messages (aggregate_id TEXT, destination TEXT, event_type TEXT);
                CREATE OR REPLACE FUNCTION copy_outbox() RETURNS trigger AS $$
                BEGIN
                    INSERT INTO sent_messages VALUES (NEW.aggregate_id, NEW.destination, NEW.event_type);
                    RETURN NEW;
                END $$ LANGUAGE plpgsql;
                CREATE OR REPLACE TRIGGER copy_outbox AFTER INSERT ON outbox_events
                    FOR EACH ROW EXECUTE FUNCTION copy_outbox();
                """);
    }

    @Test
    void sagaCompletesWhenStockAndPaymentSucceed() throws Exception {
        UUID orderId = placeOrder();
        assertThat(sentCommands(orderId)).containsExactly("ReserveStock");

        reply("inventory", orderId, StockReply.RESERVED, UUID.randomUUID());
        awaitAllHandled();
        assertThat(sagaState(orderId)).isEqualTo("CHARGING_PAYMENT");
        assertThat(sentCommands(orderId)).containsExactly("ReserveStock", "ChargePayment");

        reply("payments", orderId, PaymentReply.COMPLETED, UUID.randomUUID());
        awaitAllHandled();
        assertThat(sagaState(orderId)).isEqualTo("COMPLETED");
        assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");
    }

    @Test
    void repeatedAndLateRepliesChangeNothing() throws Exception {
        UUID orderId = placeOrder();
        UUID reservedId = UUID.randomUUID();

        // The same reply delivered twice, then a copy with a new id, as a resent compensation would have
        reply("inventory", orderId, StockReply.RESERVED, reservedId);
        reply("inventory", orderId, StockReply.RESERVED, reservedId);
        reply("inventory", orderId, StockReply.RESERVED, UUID.randomUUID());
        awaitAllHandled();
        assertThat(sentCommands(orderId)).containsExactly("ReserveStock", "ChargePayment");

        reply("payments", orderId, PaymentReply.COMPLETED, UUID.randomUUID());
        // A reply that is no longer expected: the saga has finished
        reply("inventory", orderId, StockReply.REJECTED, UUID.randomUUID());
        awaitAllHandled();

        assertThat(sagaState(orderId)).isEqualTo("COMPLETED");
        assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");
        assertThat(sentCommands(orderId)).containsExactly("ReserveStock", "ChargePayment");
    }

    // The inventory listener reads bytes, so its dead letters need a bytes serializer. With the default string
    // template the publish failed and the record was retried forever, blocking its partition.
    @Test
    void unreadableAvroReplyGoesToTheDlt() throws Exception {
        UUID orderId = placeOrder();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("inventory", orderId.toString(),
                "not-avro".getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", StockReply.RESERVED.getBytes(StandardCharsets.UTF_8));
        producer.send(record).get();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(endOffset("inventory-dlt")).isEqualTo(dltBefore.get("inventory") + 1));
        assertThat(sagaState(orderId)).isEqualTo("RESERVING_STOCK");
    }

    private UUID placeOrder() {
        return orderService.placeOrder("test", "sku-1", 1, new BigDecimal("10.00"), "EUR").getId();
    }

    private void reply(String topic, UUID orderId, String type, UUID eventId) throws Exception {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, orderId.toString(),
                payload(topic, orderId, type, eventId));
        record.headers().add("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", type.getBytes(StandardCharsets.UTF_8));
        producer.send(record).get();
    }

    private byte[] payload(String topic, UUID orderId, String type, UUID eventId) {
        if (StockReply.RESERVED.equals(type)) {
            GenericRecord record = new GenericData.Record(STOCK_RESERVED);
            record.put("eventId", eventId.toString());
            record.put("occurredAt", System.currentTimeMillis());
            record.put("orderId", orderId.toString());
            return avroSerializer.serialize(topic, record);
        }
        return """
                {"orderId":"%s"}""".formatted(orderId).getBytes(StandardCharsets.UTF_8);
    }

    // Commands only: the orders topic also gets OrderCreated
    private List<String> sentCommands(UUID orderId) {
        return jdbc.queryForList("""
                SELECT event_type FROM sent_messages WHERE aggregate_id = ? AND destination LIKE '%-commands'""",
                String.class, orderId.toString());
    }

    private String sagaState(UUID orderId) {
        return jdbc.queryForObject("SELECT state FROM order_sagas WHERE order_id = ?", String.class, orderId);
    }

    private String orderStatus(UUID orderId) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id = ?", String.class, orderId);
    }

    // Every reply committed and none on a DLT: each one ended cleanly, not through the error handler
    private void awaitAllHandled() throws Exception {
        for (String topic : REPLY_TOPICS) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(committed(topic)).isEqualTo(endOffset(topic)));
            assertThat(endOffset(topic + "-dlt")).isEqualTo(dltBefore.get(topic));
        }
    }

    // Records written to a topic, over all partitions; 0 if the topic was never created
    private long endOffset(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            if (!admin.listTopics().names().get().contains(topic)) {
                return 0;
            }
            Map<TopicPartition, OffsetSpec> request = admin.describeTopics(List.of(topic)).allTopicNames().get()
                    .get(topic).partitions().stream()
                    .collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
            return admin.listOffsets(request).all().get().values().stream().mapToLong(o -> o.offset()).sum();
        }
    }

    // Offsets this service has committed on a topic, over all partitions
    private long committed(String topic) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            return admin.listConsumerGroupOffsets("order-service").partitionsToOffsetAndMetadata().get()
                    .entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
                    .mapToLong(e -> e.getValue().offset()).sum();
        }
    }
}
