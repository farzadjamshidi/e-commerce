package com.ecommerce.order;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class PaymentReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReplyHandler.class);

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
        if (!PaymentReply.COMPLETED.equals(eventType) && !PaymentReply.FAILED.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        PaymentReply reply = jsonMapper.readValue(payload, PaymentReply.class);
        if (PaymentReply.COMPLETED.equals(eventType)) {
            orderSagaOrchestrator.onPaymentCompleted(reply.orderId());
        } else {
            orderSagaOrchestrator.onPaymentFailed(reply.orderId(), reply.reason());
        }
    }
}
