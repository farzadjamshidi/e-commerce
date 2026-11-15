package com.ecommerce.order;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

// Broadcast, not work sharing: every instance has its own consumer group, so every instance reads every event and
// tells the browsers connected to it. The saga listeners share one group, so each reply is handled once.
@Component
class OrderStatusListener {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusListener.class);
    private static final Set<String> FINISHED = Set.of(OrderFinishedEvent.CONFIRMED, OrderFinishedEvent.CANCELLED);

    private final OrderStatusStreams orderStatusStreams;
    private final JsonMapper jsonMapper;

    OrderStatusListener(OrderStatusStreams orderStatusStreams, JsonMapper jsonMapper) {
        this.orderStatusStreams = orderStatusStreams;
        this.jsonMapper = jsonMapper;
    }

    // A new group per start, reading from the end: only events from now on matter, a browser that missed one
    // gets the status from the database when it reconnects
    @KafkaListener(id = "order-status-stream", idIsGroup = false, topics = "orders",
            groupId = "order-service-status-${random.uuid}",
            properties = "auto.offset.reset=latest")
    void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // Never thrown to the error handler: a lost notice costs a reconnect, but a retried one would block the
        // partition, and the dead letter topic orders-dlt does not exist
        try {
            if (FINISHED.contains(header(record, "eventType"))) {
                OrderFinishedEvent event = jsonMapper.readValue(record.value(), OrderFinishedEvent.class);
                orderStatusStreams.send(event.orderId(), event.status());
            }
        } catch (RuntimeException e) {
            log.warn("Order event skipped <- partition {} offset {}: {}", record.partition(), record.offset(),
                    e.getMessage());
        }
        ack.acknowledge();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
