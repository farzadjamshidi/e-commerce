package com.ecommerce.order;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class OrderService {

    private static final String AGGREGATE_TYPE = "Order";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;
    private final boolean relayEnabled;

    OrderService(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository,
                 JsonMapper jsonMapper, @Value("${outbox.relay.enabled:false}") boolean relayEnabled) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
        this.relayEnabled = relayEnabled;
    }

    // The order and its event are committed together, or not at all
    @Transactional
    Order placeOrder(String customerId, String sku, int quantity, BigDecimal unitPrice, String currency) {
        Order order = orderRepository.save(Order.place(customerId, sku, quantity, unitPrice, currency));

        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        // Flush runs the INSERT now, which fires @PostPersist and clears isNew.
        // With a plain save, delete() would see a "new" entity and silently do nothing.
        OutboxEvent outboxEvent = outboxEventRepository.saveAndFlush(OutboxEvent.create(event.eventId(),
                AGGREGATE_TYPE, order.getId().toString(), event.eventType(), jsonMapper.writeValueAsString(event),
                event.occurredAt()));

        // Debezium reads the INSERT from the WAL, so the row itself is no longer needed.
        // The polling relay reads the table, so the row must stay while it is in use.
        if (!relayEnabled) {
            outboxEventRepository.delete(outboxEvent);
        }

        return order;
    }

    @Transactional(readOnly = true)
    Optional<Order> findOrder(UUID id) {
        return orderRepository.findById(id);
    }
}
