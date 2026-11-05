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

    OrderSagaOrchestrator(OrderSagaRepository orderSagaRepository, OrderRepository orderRepository,
                          OutboxWriter outboxWriter) {
        this.orderSagaRepository = orderSagaRepository;
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
    }

    // Joins placeOrder's transaction: no order without a saga, no saga without its first command
    @Transactional(propagation = Propagation.MANDATORY)
    void start(Order order) {
        orderSagaRepository.insert(order.getId(), SagaState.RESERVING_STOCK.name());
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

    private boolean moved(UUID orderId, SagaState from, SagaState to) {
        if (orderSagaRepository.transition(orderId, from.name(), to.name()) == 1) {
            return true;
        }
        log.warn("Order {}: saga is not in {}, reply ignored", orderId, from);
        return false;
    }
}
