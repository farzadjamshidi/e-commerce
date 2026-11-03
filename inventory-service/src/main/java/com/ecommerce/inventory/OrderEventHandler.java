package com.ecommerce.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
class OrderEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);
    private static final String AGGREGATE_TYPE = "Reservation";

    private final ProcessedEventRepository processedEventRepository;
    private final StockRepository stockRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxWriter outboxWriter;
    private final JsonMapper jsonMapper;

    OrderEventHandler(ProcessedEventRepository processedEventRepository, StockRepository stockRepository,
                      ReservationRepository reservationRepository, OutboxWriter outboxWriter,
                      JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.outboxWriter = outboxWriter;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row, the stock change, the reservation row and the outgoing event commit together
    @Transactional
    void handle(UUID eventId, String eventType, String payload) {
        // Checked before the inbox: an ignored type must not be marked processed,
        // or it would be skipped as a duplicate once this service starts handling it
        if (!OrderCreatedEvent.TYPE.equals(eventType) && !OrderCancelledEvent.TYPE.equals(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        if (OrderCreatedEvent.TYPE.equals(eventType)) {
            reserve(jsonMapper.readValue(payload, OrderCreatedEvent.class));
        } else {
            release(jsonMapper.readValue(payload, OrderCancelledEvent.class));
        }
    }

    private void reserve(OrderCreatedEvent event) {
        if (stockRepository.reserve(event.sku(), event.quantity()) == 1) {
            reservationRepository.insertReserved(event.orderId(), event.sku(), event.quantity());
            InventoryReservedEvent reserved = InventoryReservedEvent.from(event);
            outboxWriter.write(reserved.eventId(), AGGREGATE_TYPE, event.orderId().toString(), reserved.eventType(),
                    reserved);
            log.info("Reserved {} x {} for order {}", event.quantity(), event.sku(), event.orderId());
        } else {
            // A business outcome, not a failure: retrying will not create stock
            InventoryRejectedEvent rejected = InventoryRejectedEvent.insufficientStock(event);
            outboxWriter.write(rejected.eventId(), AGGREGATE_TYPE, event.orderId().toString(), rejected.eventType(),
                    rejected);
            log.warn("Insufficient stock for {} x {}, order {}", event.quantity(), event.sku(), event.orderId());
        }
    }

    // OrderCancelled has the same key as OrderCreated, so it always comes after it: the reservation, if any, exists
    private void release(OrderCancelledEvent event) {
        if (reservationRepository.release(event.orderId()) == 1) {
            log.info("Released reservation of cancelled order {} ({})", event.orderId(), event.reason());
        } else {
            log.info("Cancelled order {} has no active reservation", event.orderId());
        }
    }
}
