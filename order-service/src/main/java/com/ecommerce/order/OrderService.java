package com.ecommerce.order;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class OrderService {

    private static final String ORDERS_TOPIC = "orders";

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final OrderSagaOrchestrator orderSagaOrchestrator;

    OrderService(OrderRepository orderRepository, OutboxWriter outboxWriter,
                 OrderSagaOrchestrator orderSagaOrchestrator) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
        this.orderSagaOrchestrator = orderSagaOrchestrator;
    }

    // The order, its event, the saga row and the first command are committed together, or not at all
    @Transactional
    Order placeOrder(String customerId, String sku, int quantity, BigDecimal unitPrice, String currency) {
        Order order = orderRepository.save(Order.place(customerId, sku, quantity, unitPrice, currency));

        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        outboxWriter.write(event.eventId(), ORDERS_TOPIC, order.getId(), event.eventType(), event,
                event.occurredAt());
        orderSagaOrchestrator.start(order);

        return order;
    }

    @Transactional(readOnly = true)
    Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }
}
