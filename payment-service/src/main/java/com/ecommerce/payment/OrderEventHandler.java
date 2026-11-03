package com.ecommerce.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
class OrderEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private final JsonMapper jsonMapper;

    OrderEventHandler(ProcessedEventRepository processedEventRepository, PaymentRepository paymentRepository,
                      JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
        this.jsonMapper = jsonMapper;
    }

    // OrderCancelled is on another topic than InventoryReserved, so it can come before or after the charge
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        if (!OrderCancelledEvent.TYPE.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        OrderCancelledEvent event = jsonMapper.readValue(payload, OrderCancelledEvent.class);
        paymentRepository.cancel(event.orderId(), event.reason()).ifPresentOrElse(
                status -> log.info("Order {} cancelled ({}): payment {}", event.orderId(), event.reason(), status),
                () -> log.info("Order {} cancelled ({}): not charged, nothing to do", event.orderId(),
                        event.reason()));
    }
}
