package com.ecommerce.order;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

// The orchestrator knows which step each saga waits for, so a timeout can act on that step
@Component
class SagaTimeout {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeout.class);

    private final OrderSagaRepository orderSagaRepository;
    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final Duration after;
    private final int batchSize;

    SagaTimeout(OrderSagaRepository orderSagaRepository, OrderSagaOrchestrator orderSagaOrchestrator,
                @Value("${order.saga-timeout.after}") Duration after,
                @Value("${order.saga-timeout.batch-size}") int batchSize) {
        this.orderSagaRepository = orderSagaRepository;
        this.orderSagaOrchestrator = orderSagaOrchestrator;
        this.after = after;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${order.saga-timeout.check-interval-ms}")
    void checkStuckSagas() {
        Instant before = Instant.now().minus(after);
        check(SagaState.RESERVING_STOCK, before, orderSagaOrchestrator::onReservationTimeout);
        check(SagaState.CHARGING_PAYMENT, before, orderSagaOrchestrator::onPaymentTimeout);
        check(SagaState.REFUNDING_PAYMENT, before, orderSagaOrchestrator::onRefundTimeout);
        check(SagaState.RELEASING_STOCK, before, orderSagaOrchestrator::onReleaseTimeout);
    }

    // One transaction per saga: a failure does not undo the others
    private void check(SagaState state, Instant before, Consumer<UUID> action) {
        List<UUID> stuck = orderSagaRepository.findStuck(state.name(), before, batchSize);
        stuck.forEach(action);
        if (!stuck.isEmpty()) {
            log.info("Timeout: {} sagas in {} longer than {}", stuck.size(), state, after);
        }
    }
}
