package com.ecommerce.order;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class SagaEventHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaEventHandler.class);

    // The events that end the saga. InventoryReserved is a middle step, so it is not here.
    private static final Map<String, OrderStatus> FINAL_STATUS = Map.of(
            "PaymentCompleted", OrderStatus.CONFIRMED,
            "InventoryRejected", OrderStatus.CANCELLED,
            "PaymentFailed", OrderStatus.CANCELLED);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderRepository orderRepository;
    private final JsonMapper jsonMapper;

    SagaEventHandler(ProcessedEventRepository processedEventRepository, OrderRepository orderRepository,
                     JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.orderRepository = orderRepository;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row and the status change commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        OrderStatus status = FINAL_STATUS.get(eventType);
        if (status == null) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        SagaResultEvent event = jsonMapper.readValue(payload, SagaResultEvent.class);
        if (orderRepository.completePending(event.orderId(), status.name()) == 1) {
            log.info("Order {} {} after {}{}", event.orderId(), status, eventType,
                    event.reason() == null ? "" : " (" + event.reason() + ")");
        } else {
            log.warn("Order {} not PENDING, {} ignored", event.orderId(), eventType);
        }
    }
}
