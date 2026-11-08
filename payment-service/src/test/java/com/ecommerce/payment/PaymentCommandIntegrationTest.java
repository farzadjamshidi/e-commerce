package com.ecommerce.payment;

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
class PaymentCommandIntegrationTest {

    private static final String TOPIC = "payment-commands";

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
    void chargeCompletesPayment() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendCharge(orderId, "100.00");

        awaitAllHandled();
        assertThat(paymentStatus(orderId)).isEqualTo("COMPLETED");
    }

    @Test
    void refundAfterChargeRefundsPayment() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendCharge(orderId, "100.00");
        sendRefund(orderId);

        awaitAllHandled();
        assertThat(paymentStatus(orderId)).isEqualTo("REFUNDED");
    }

    // The saga sends a refund after a timeout even when no charge happened. It must be a clean no-op.
    @Test
    void refundWithoutChargeChangesNothing() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendRefund(orderId);

        awaitAllHandled();
        assertThat(paymentStatus(orderId)).isNull();
    }

    // Cannot happen in the normal flow, where both commands share a partition and the refund comes second.
    // It can when a ChargePayment is replayed by hand from the DLT after the saga gave up.
    @Test
    void chargeAfterRefundTakesNoMoney() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendRefund(orderId);
        sendCharge(orderId, "100.00");

        awaitAllHandled();
        assertThat(paymentStatus(orderId)).isNull();
    }

    private void sendCharge(UUID orderId, String amount) {
        send(orderId, ChargePaymentCommand.TYPE, """
                {"orderId":"%s","customerId":"test","amount":%s,"currency":"EUR"}""".formatted(orderId, amount));
    }

    private void sendRefund(UUID orderId) {
        send(orderId, RefundPaymentCommand.TYPE, """
                {"orderId":"%s"}""".formatted(orderId));
    }

    // Same key as the saga uses, so both commands of an order stay in one partition, in order
    private void send(UUID orderId, String type, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, orderId.toString(), payload);
        record.headers().add("eventId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", type.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).join();
    }

    // Every record committed and none on the DLT: each command ended cleanly, not through the error handler
    private void awaitAllHandled() throws Exception {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(committed(TOPIC)).isEqualTo(endOffset(TOPIC)));
        assertThat(endOffset(TOPIC + "-dlt")).isZero();
    }

    private String paymentStatus(UUID orderId) {
        return jdbc.query("SELECT status FROM payments WHERE order_id = ?",
                rs -> rs.next() ? rs.getString(1) : null, orderId);
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
            return admin.listConsumerGroupOffsets("payment-service").partitionsToOffsetAndMetadata().get()
                    .entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
                    .mapToLong(e -> e.getValue().offset()).sum();
        }
    }
}
