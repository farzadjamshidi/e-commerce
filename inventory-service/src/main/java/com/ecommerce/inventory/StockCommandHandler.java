package com.ecommerce.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
class StockCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(StockCommandHandler.class);
    private static final String AGGREGATE_TYPE = "Reservation";

    private final ProcessedEventRepository processedEventRepository;
    private final StockRepository stockRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxWriter outboxWriter;
    private final JsonMapper jsonMapper;

    StockCommandHandler(ProcessedEventRepository processedEventRepository, StockRepository stockRepository,
                        ReservationRepository reservationRepository, OutboxWriter outboxWriter,
                        JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.outboxWriter = outboxWriter;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row, the stock change, the reservation row and the reply commit together
    @Transactional
    void handle(UUID commandId, String commandType, String payload) {
        // Checked before the inbox: an ignored type must not be marked processed,
        // or it would be skipped as a duplicate once this service starts handling it
        if (!ReserveStockCommand.TYPE.equals(commandType)) {
            log.info("Ignored {} {}", commandType, commandId);
            return;
        }
        if (processedEventRepository.markProcessed(commandId, commandType) == 0) {
            log.info("Duplicate {} {}, skipped", commandType, commandId);
            return;
        }
        ReserveStockCommand command = jsonMapper.readValue(payload, ReserveStockCommand.class);
        String orderId = command.orderId().toString();
        if (stockRepository.reserve(command.sku(), command.quantity()) == 1) {
            reservationRepository.insertReserved(command.orderId(), command.sku(), command.quantity());
            StockReservedEvent reserved = StockReservedEvent.from(command);
            outboxWriter.write(reserved.eventId(), AGGREGATE_TYPE, orderId, reserved.eventType(), reserved);
            log.info("Reserved {} x {} for order {}", command.quantity(), command.sku(), orderId);
        } else {
            // A business outcome, not a failure: retrying will not create stock
            StockRejectedEvent rejected = StockRejectedEvent.insufficientStock(command);
            outboxWriter.write(rejected.eventId(), AGGREGATE_TYPE, orderId, rejected.eventType(), rejected);
            log.warn("Insufficient stock for {} x {}, order {}", command.quantity(), command.sku(), orderId);
        }
    }
}
