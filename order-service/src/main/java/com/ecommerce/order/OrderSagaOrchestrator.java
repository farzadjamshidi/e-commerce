package com.ecommerce.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);
    private static final String INVENTORY_COMMANDS_TOPIC = "inventory-commands";
    private static final String PAYMENT_COMMANDS_TOPIC = "payment-commands";

    private final OrderSagaRepository orderSagaRepository;
    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final TraceContext traceContext;

    OrderSagaOrchestrator(OrderSagaRepository orderSagaRepository, OrderRepository orderRepository,
                          OutboxWriter outboxWriter, TraceContext traceContext) {
        this.orderSagaRepository = orderSagaRepository;
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
        this.traceContext = traceContext;
    }

    // Joins placeOrder's transaction: no order without a saga, no saga without its first command
    @Transactional(propagation = Propagation.MANDATORY)
    void start(Order order) {
        orderSagaRepository.insert(order.getId(), SagaState.RESERVING_STOCK.name(), traceContext.current());
        ReserveStockCommand command = ReserveStockCommand.from(order);
        outboxWriter.write(command.commandId(), INVENTORY_COMMANDS_TOPIC, order.getId(), command.commandType(),
                command, command.issuedAt());
        log.info("Saga started for order {}: {}", order.getId(), SagaState.RESERVING_STOCK);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    void onStockReserved(UUID orderId) {
        if (!moved(orderId, SagaState.RESERVING_STOCK, SagaState.CHARGING_PAYMENT)) {
            return;
        }
        ChargePaymentCommand command = ChargePaymentCommand.from(orderRepository.findById(orderId).orElseThrow());
        outboxWriter.write(command.commandId(), PAYMENT_COMMANDS_TOPIC, orderId, command.commandType(), command,
                command.issuedAt());
        log.info("Order {}: stock reserved, charging {} {}", orderId, command.amount(), command.currency());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    void onStockRejected(UUID orderId, String reason) {
        if (!moved(orderId, SagaState.RESERVING_STOCK, SagaState.CANCELLED)) {
            return;
        }
        // Nothing was reserved or charged, so there is nothing to compensate
        orderRepository.completePending(orderId, OrderStatus.CANCELLED.name());
        log.info("Order {}: stock rejected ({}), cancelled", orderId, reason);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    void onPaymentCompleted(UUID orderId) {
        if (!moved(orderId, SagaState.CHARGING_PAYMENT, SagaState.COMPLETED)) {
            return;
        }
        orderRepository.completePending(orderId, OrderStatus.CONFIRMED.name());
        log.info("Order {}: payment completed, confirmed", orderId);
    }

    // The order stays PENDING until the stock is back: CANCELLED means the compensation is finished
    @Transactional(propagation = Propagation.MANDATORY)
    void onPaymentFailed(UUID orderId, String reason) {
        if (!moved(orderId, SagaState.CHARGING_PAYMENT, SagaState.RELEASING_STOCK)) {
            return;
        }
        sendReleaseStock(orderId);
        log.info("Order {}: payment failed ({}), releasing stock", orderId, reason);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    void onStockReleased(UUID orderId) {
        if (!moved(orderId, SagaState.RELEASING_STOCK, SagaState.CANCELLED)) {
            return;
        }
        orderRepository.completePending(orderId, OrderStatus.CANCELLED.name());
        log.info("Order {}: stock released, cancelled", orderId);
    }

    // Compensations run in reverse order: the money first, then the stock
    @Transactional(propagation = Propagation.MANDATORY)
    void onPaymentRefunded(UUID orderId) {
        if (moved(orderId, SagaState.REFUNDING_PAYMENT, SagaState.RELEASING_STOCK)) {
            sendReleaseStock(orderId);
            log.info("Order {}: payment refunded, releasing stock", orderId);
        }
    }

    // No reply yet, so inventory may or may not have reserved. ReleaseStock has the same key as ReserveStock,
    // so inventory reads it afterwards. A late StockReserved finds the saga in RELEASING_STOCK and is ignored.
    @Transactional
    void onReservationTimeout(UUID orderId) {
        if (moved(orderId, SagaState.RESERVING_STOCK, SagaState.RELEASING_STOCK)) {
            sendReleaseStock(orderId);
            log.warn("Order {}: no reply from inventory, releasing stock", orderId);
        }
    }

    // A compensation must finish, so it is sent again until inventory answers.
    // A new command ID on purpose: the inbox would swallow the old one, and release is idempotent by status.
    @Transactional
    void onReleaseTimeout(UUID orderId) {
        if (moved(orderId, SagaState.RELEASING_STOCK, SagaState.RELEASING_STOCK)) {
            sendReleaseStock(orderId);
            log.warn("Order {}: still no StockReleased, sending ReleaseStock again", orderId);
        }
    }

    // No reply yet, so the customer may or may not be charged. RefundPayment has the same key as ChargePayment,
    // so payment reads it afterwards. A late PaymentCompleted finds the saga in REFUNDING_PAYMENT and is ignored.
    @Transactional
    void onPaymentTimeout(UUID orderId) {
        if (moved(orderId, SagaState.CHARGING_PAYMENT, SagaState.REFUNDING_PAYMENT)) {
            sendRefundPayment(orderId);
            log.warn("Order {}: no reply from payment, refunding", orderId);
        }
    }

    // Sent again with a new command ID, for the same reason as ReleaseStock: refund is idempotent by status
    @Transactional
    void onRefundTimeout(UUID orderId) {
        if (moved(orderId, SagaState.REFUNDING_PAYMENT, SagaState.REFUNDING_PAYMENT)) {
            sendRefundPayment(orderId);
            log.warn("Order {}: still no PaymentRefunded, sending RefundPayment again", orderId);
        }
    }

    private void sendRefundPayment(UUID orderId) {
        RefundPaymentCommand command = RefundPaymentCommand.of(orderId);
        outboxWriter.write(command.commandId(), PAYMENT_COMMANDS_TOPIC, orderId, command.commandType(), command,
                command.issuedAt());
    }

    private void sendReleaseStock(UUID orderId) {
        ReleaseStockCommand command = ReleaseStockCommand.of(orderId);
        outboxWriter.write(command.commandId(), INVENTORY_COMMANDS_TOPIC, orderId, command.commandType(), command,
                command.issuedAt());
    }

    private boolean moved(UUID orderId, SagaState from, SagaState to) {
        if (orderSagaRepository.transition(orderId, from.name(), to.name()) == 1) {
            return true;
        }
        log.warn("Order {}: saga is not in {}, reply ignored", orderId, from);
        return false;
    }
}
