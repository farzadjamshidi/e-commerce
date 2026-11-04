package com.ecommerce.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);
    private static final String INVENTORY_COMMANDS_TOPIC = "inventory-commands";

    private final OrderSagaRepository orderSagaRepository;
    private final OutboxWriter outboxWriter;

    OrderSagaOrchestrator(OrderSagaRepository orderSagaRepository, OutboxWriter outboxWriter) {
        this.orderSagaRepository = orderSagaRepository;
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
}
