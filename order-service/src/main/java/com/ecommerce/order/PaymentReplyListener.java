package com.ecommerce.order;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class PaymentReplyListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentReplyListener.class);

    private final PaymentReplyHandler paymentReplyHandler;

    PaymentReplyListener(PaymentReplyHandler paymentReplyHandler) {
        this.paymentReplyHandler = paymentReplyHandler;
    }

    @KafkaListener(id = "payment-replies", idIsGroup = false, topics = "payments")
    void onPaymentReply(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String eventId = header(record, "eventId");
        if (eventId == null) {
            log.warn("No eventId header <- partition {} offset {}, skipped", record.partition(), record.offset());
        } else {
            paymentReplyHandler.handle(UUID.fromString(eventId), header(record, "eventType"), record.value());
        }
        // Acknowledged only after the database commit; the inbox absorbs redelivery
        ack.acknowledge();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
