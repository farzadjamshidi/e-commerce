package com.ecommerce.order;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class OrderService {

    private static final String AGGREGATE_TYPE = "Order";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    OrderService(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository,
                 JsonMapper jsonMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
    }

    // The order and its event are committed together, or not at all
    @Transactional
    Order placeOrder(String customerId, String sku, int quantity, BigDecimal unitPrice, String currency) {
        Order order = orderRepository.save(Order.place(customerId, sku, quantity, unitPrice, currency));

        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        outboxEventRepository.save(OutboxEvent.create(event.eventId(), AGGREGATE_TYPE, order.getId().toString(),
                event.eventType(), jsonMapper.writeValueAsString(event), event.occurredAt()));

        return order;
    }

    @Transactional(readOnly = true)
    Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }
}
