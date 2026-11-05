package com.ecommerce.order;

import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class PaymentReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReplyHandler.class);
    private static final Set<String> HANDLED =
            Set.of(PaymentReply.COMPLETED, PaymentReply.FAILED, PaymentReply.REFUNDED);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final JsonMapper jsonMapper;

    PaymentReplyHandler(ProcessedEventRepository processedEventRepository,
                        OrderSagaOrchestrator orderSagaOrchestrator, JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.orderSagaOrchestrator = orderSagaOrchestrator;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row, the saga step and the next command commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        if (!HANDLED.contains(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        PaymentReply reply = jsonMapper.readValue(payload, PaymentReply.class);
        switch (eventType) {
            case PaymentReply.COMPLETED -> orderSagaOrchestrator.onPaymentCompleted(reply.orderId());
            case PaymentReply.FAILED -> orderSagaOrchestrator.onPaymentFailed(reply.orderId(), reply.reason());
            default -> orderSagaOrchestrator.onPaymentRefunded(reply.orderId());
        }
    }
}
