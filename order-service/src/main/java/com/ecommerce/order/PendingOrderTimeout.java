package com.ecommerce.order;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

// In choreography no service owns the saga, so nothing notices an order whose final event never comes
@Component
class PendingOrderTimeout {

    private static final Logger log = LoggerFactory.getLogger(PendingOrderTimeout.class);

    private final OrderRepository orderRepository;
    private final OrderService orderService;
    private final Duration after;
    private final int batchSize;

    PendingOrderTimeout(OrderRepository orderRepository, OrderService orderService,
                        @Value("${order.pending-timeout.after}") Duration after,
                        @Value("${order.pending-timeout.batch-size}") int batchSize) {
        this.orderRepository = orderRepository;
        this.orderService = orderService;
        this.after = after;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${order.pending-timeout.check-interval-ms}")
    void cancelTimedOut() {
        List<UUID> stuck = orderRepository.findPendingCreatedBefore(Instant.now().minus(after), batchSize);
        int cancelled = 0;
        // One transaction per order: a failure does not undo the others
        for (UUID orderId : stuck) {
            if (orderService.cancelIfPending(orderId, OrderCancelledEvent.TIMEOUT)) {
                cancelled++;
            }
        }
        if (!stuck.isEmpty()) {
            log.info("Timeout: cancelled {} of {} orders PENDING longer than {}", cancelled, stuck.size(), after);
        }
    }
}
