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
class PaymentCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);
    private static final String AGGREGATE_TYPE = "Payment";

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private final VoidedOrderRepository voidedOrderRepository;
    private final OutboxWriter outboxWriter;
    private final JsonMapper jsonMapper;
    private final BigDecimal maxAmount;

    PaymentCommandHandler(ProcessedEventRepository processedEventRepository, PaymentRepository paymentRepository,
                          VoidedOrderRepository voidedOrderRepository, OutboxWriter outboxWriter, JsonMapper jsonMapper,
                          @Value("${payment.max-amount}") BigDecimal maxAmount) {
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
        this.voidedOrderRepository = voidedOrderRepository;
        this.outboxWriter = outboxWriter;
        this.jsonMapper = jsonMapper;
        this.maxAmount = maxAmount;
    }

    // The inbox row, the payment row and the reply commit together
    @Transactional
    void handle(UUID commandId, String commandType, String payload) {
        if (!ChargePaymentCommand.TYPE.equals(commandType) && !RefundPaymentCommand.TYPE.equals(commandType)) {
            log.info("Ignored {} {}", commandType, commandId);
            return;
        }
        if (processedEventRepository.markProcessed(commandId, commandType) == 0) {
            log.info("Duplicate {} {}, skipped", commandType, commandId);
            return;
        }
        if (ChargePaymentCommand.TYPE.equals(commandType)) {
            charge(jsonMapper.readValue(payload, ChargePaymentCommand.class));
        } else {
            refund(jsonMapper.readValue(payload, RefundPaymentCommand.class));
        }
    }

    private void charge(ChargePaymentCommand command) {
        String orderId = command.orderId().toString();
        // No reply: the saga that sent the refund is already over, so nobody waits for this charge.
        // Check and insert cannot race, because both commands of an order are handled in one partition.
        if (voidedOrderRepository.existsByOrderId(command.orderId())) {
            log.warn("Order {} was refunded before this charge arrived, not charged", orderId);
            return;
        }
        // Simulated gateway: a deterministic rule makes the failure path easy to trigger
        boolean approved = command.amount().compareTo(maxAmount) <= 0;
        String reason = approved ? null : PaymentFailedEvent.AMOUNT_LIMIT_EXCEEDED;
        PaymentStatus status = approved ? PaymentStatus.COMPLETED : PaymentStatus.FAILED;
        if (paymentRepository.insertIfAbsent(command.orderId(), command.customerId(), command.amount(),
                command.currency(), status.name(), reason) == 0) {
            log.warn("Order {} already has a payment, not charged", orderId);
            return;
        }
        if (approved) {
            PaymentCompletedEvent completed = PaymentCompletedEvent.from(command);
            outboxWriter.write(completed.eventId(), AGGREGATE_TYPE, orderId, completed.eventType(), completed);
            log.info("Charged {} {} for order {}", command.amount(), command.currency(), orderId);
        } else {
            PaymentFailedEvent failed = PaymentFailedEvent.amountLimitExceeded(command);
            outboxWriter.write(failed.eventId(), AGGREGATE_TYPE, orderId, failed.eventType(), failed);
            log.warn("Payment failed for order {}: {} {} over limit {}", orderId, command.amount(),
                    command.currency(), maxAmount);
        }
    }

    // Always answers, even with nothing to refund: the saga is waiting. RefundPayment has the same key as
    // ChargePayment, so the charge was already handled here, and a failed one simply has nothing to give back.
    private void refund(RefundPaymentCommand command) {
        int refunded = paymentRepository.refund(command.orderId());
        voidedOrderRepository.insert(command.orderId());
        PaymentRefundedEvent reply = PaymentRefundedEvent.from(command);
        outboxWriter.write(reply.eventId(), AGGREGATE_TYPE, command.orderId().toString(), reply.eventType(), reply);
        log.info("Refund for order {}: {} payment refunded", command.orderId(), refunded);
    }
}
