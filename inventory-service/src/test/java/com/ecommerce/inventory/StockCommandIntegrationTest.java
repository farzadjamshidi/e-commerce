package com.ecommerce.inventory;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Real Postgres and Kafka in containers. Debezium is not part of it, so replies are not checked here.
@SpringBootTest(properties = "management.tracing.export.enabled=false")
@Testcontainers
class StockCommandIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.1")
            .withInitScript("create-debezium-role.sql");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Test
    void reserveStockCommandReservesStock() {
        String sku = newSku(5);
        UUID orderId = UUID.randomUUID();

        sendReserveStock(UUID.randomUUID(), orderId, sku, 2);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(reservationStatus(orderId)).isEqualTo("RESERVED"));
        assertThat(stock(sku)).isEqualTo("3/2");
    }

    @Test
    void sameCommandTwiceReservesOnce() throws Exception {
        String sku = newSku(5);
        UUID orderId = UUID.randomUUID();
        UUID commandId = UUID.randomUUID();

        sendReserveStock(commandId, orderId, sku, 2);
        sendReserveStock(commandId, orderId, sku, 2);

        // The copy must be a clean skip. Without the inbox the reservation key would still keep the stock right,
        // but the copy would fail, be retried for about 7 s and land on the DLT.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(committed("inventory-commands")).isEqualTo(endOffset("inventory-commands")));
        assertThat(endOffset("inventory-commands-dlt")).isZero();
        assertThat(stock(sku)).isEqualTo("3/2");
    }

    private String newSku(int available) {
        String sku = "test-" + UUID.randomUUID();
        jdbc.update("INSERT INTO stock (sku, available) VALUES (?, ?)", sku, available);
        return sku;
    }

    private void sendReserveStock(UUID commandId, UUID orderId, String sku, int quantity) {
        String payload = """
                {"orderId":"%s","sku":"%s","quantity":%d}""".formatted(orderId, sku, quantity);
        ProducerRecord<String, String> record = new ProducerRecord<>("inventory-commands", orderId.toString(), payload);
        record.headers().add("eventId", commandId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", ReserveStockCommand.TYPE.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).join();
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
            return admin.listConsumerGroupOffsets("inventory-service").partitionsToOffsetAndMetadata().get()
                    .entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
                    .mapToLong(e -> e.getValue().offset()).sum();
        }
    }

    private String reservationStatus(UUID orderId) {
        return jdbc.query("SELECT status FROM reservations WHERE order_id = ?",
                rs -> rs.next() ? rs.getString(1) : null, orderId);
    }

    private String stock(String sku) {
        return jdbc.queryForObject("SELECT available || '/' || reserved FROM stock WHERE sku = ?", String.class, sku);
    }
}
