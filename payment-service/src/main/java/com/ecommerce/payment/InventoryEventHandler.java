package com.ecommerce.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.UUID;

@Service
class InventoryEventHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventHandler.class);
    private static final String AGGREGATE_TYPE = "Payment";

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxWriter outboxWriter;
    private final JsonMapper jsonMapper;
    private final BigDecimal maxAmount;

    InventoryEventHandler(ProcessedEventRepository processedEventRepository, PaymentRepository paymentRepository,
                          OutboxWriter outboxWriter, JsonMapper jsonMapper,
                          @Value("${payment.max-amount}") BigDecimal maxAmount) {
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
        this.outboxWriter = outboxWriter;
        this.jsonMapper = jsonMapper;
        this.maxAmount = maxAmount;
    }

    // The inbox row, the payment row and the outgoing event commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        // InventoryRejected and other types are not for payment
        if (!InventoryReservedEvent.TYPE.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        InventoryReservedEvent reserved = jsonMapper.readValue(payload, InventoryReservedEvent.class);
        String orderId = reserved.orderId().toString();
        // Simulated gateway: a deterministic rule makes the failure path easy to trigger
        if (reserved.totalAmount().compareTo(maxAmount) <= 0) {
            if (paymentRepository.insertIfAbsent(reserved.orderId(), reserved.customerId(), reserved.totalAmount(),
                    reserved.currency(), PaymentStatus.COMPLETED.name(), null) == 0) {
                skipExisting(orderId);
                return;
            }
            PaymentCompletedEvent completed = PaymentCompletedEvent.from(reserved);
            outboxWriter.write(completed.eventId(), AGGREGATE_TYPE, orderId, completed.eventType(), completed);
            log.info("Charged {} {} for order {}", reserved.totalAmount(), reserved.currency(), orderId);
        } else {
            PaymentFailedEvent failed = PaymentFailedEvent.amountLimitExceeded(reserved);
            if (paymentRepository.insertIfAbsent(reserved.orderId(), reserved.customerId(), reserved.totalAmount(),
                    reserved.currency(), PaymentStatus.FAILED.name(), failed.reason()) == 0) {
                skipExisting(orderId);
                return;
            }
            outboxWriter.write(failed.eventId(), AGGREGATE_TYPE, orderId, failed.eventType(), failed);
            log.warn("Payment failed for order {}: {} {} over limit {}", orderId, reserved.totalAmount(),
                    reserved.currency(), maxAmount);
        }
    }

    // The row is written before the charge, so an order cancelled first is never charged
    private static void skipExisting(String orderId) {
        log.warn("Order {} already has a payment row (cancelled or handled), not charged", orderId);
    }
}
