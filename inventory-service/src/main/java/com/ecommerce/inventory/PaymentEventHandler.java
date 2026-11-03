package com.ecommerce.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
class PaymentEventHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventHandler.class);

    private final ProcessedEventRepository processedEventRepository;
    private final ReservationRepository reservationRepository;
    private final JsonMapper jsonMapper;

    PaymentEventHandler(ProcessedEventRepository processedEventRepository,
                        ReservationRepository reservationRepository, JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.reservationRepository = reservationRepository;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row and the release commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        if (!PaymentFailedEvent.TYPE.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        PaymentFailedEvent event = jsonMapper.readValue(payload, PaymentFailedEvent.class);
        if (reservationRepository.release(event.orderId()) == 1) {
            log.info("Released reservation of order {} ({})", event.orderId(), event.reason());
        } else {
            log.warn("No active reservation for order {}, nothing to release", event.orderId());
        }
    }
}
