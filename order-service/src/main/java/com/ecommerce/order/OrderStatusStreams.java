package com.ecommerce.order;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

// The open status streams of this instance, by order. Only a notice to the browser: the database stays the truth,
// and a browser that reconnects gets the current status again.
@Component
class OrderStatusStreams {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusStreams.class);

    private final Map<UUID, Set<SseEmitter>> streams = new ConcurrentHashMap<>();
    private final Duration timeout;

    OrderStatusStreams(@Value("${order.status-stream.timeout}") Duration timeout) {
        this.timeout = timeout;
    }

    SseEmitter open(UUID orderId) {
        // After the timeout the browser's EventSource reconnects by itself
        SseEmitter emitter = new SseEmitter(timeout.toMillis());
        streams.computeIfAbsent(orderId, id -> ConcurrentHashMap.newKeySet()).add(emitter);
        Runnable remove = () -> streams.computeIfPresent(orderId, (id, set) -> {
            set.remove(emitter);
            return set.isEmpty() ? null : set;
        });
        emitter.onCompletion(remove);
        emitter.onTimeout(remove);
        emitter.onError(error -> remove.run());
        return emitter;
    }

    void send(UUID orderId, OrderStatus status) {
        Set<SseEmitter> emitters = streams.get(orderId);
        if (emitters != null) {
            emitters.forEach(emitter -> send(emitter, orderId, status));
        }
    }

    void send(SseEmitter emitter, UUID orderId, OrderStatus status) {
        try {
            emitter.send(SseEmitter.event().name("status").data(new StatusMessage(orderId, status)));
            if (status != OrderStatus.PENDING) {
                emitter.complete();
            }
        } catch (IOException | IllegalStateException e) {
            // The browser has gone, or the stream was already closed
            log.debug("Status stream for order {} closed: {}", orderId, e.getMessage());
            emitter.completeWithError(e);
        }
    }

    record StatusMessage(UUID orderId, OrderStatus status) {
    }
}
