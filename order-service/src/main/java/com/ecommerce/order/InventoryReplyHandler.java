package com.ecommerce.order;

import java.util.Set;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
class InventoryReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryReplyHandler.class);
    private static final Set<String> HANDLED = Set.of(StockReply.RESERVED, StockReply.REJECTED, StockReply.RELEASED);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final AvroDecoder avroDecoder;
    private final JsonMapper jsonMapper;

    InventoryReplyHandler(ProcessedEventRepository processedEventRepository,
                          OrderSagaOrchestrator orderSagaOrchestrator, AvroDecoder avroDecoder,
                          JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.orderSagaOrchestrator = orderSagaOrchestrator;
        this.avroDecoder = avroDecoder;
        this.jsonMapper = jsonMapper;
    }

    // The inbox row, the saga step and the next command commit together
    @Transactional
    void handle(UUID eventId, String eventType, String topic, byte[] payload) {
        if (!HANDLED.contains(eventType)) {
            log.info("Ignored {} {}", eventType, eventId);
            return;
        }
        if (processedEventRepository.markProcessed(eventId, eventType) == 0) {
            log.info("Duplicate {} {}, skipped", eventType, eventId);
            return;
        }
        StockReply reply = read(eventType, topic, payload);
        switch (eventType) {
            case StockReply.RESERVED -> orderSagaOrchestrator.onStockReserved(reply.orderId());
            case StockReply.REJECTED -> orderSagaOrchestrator.onStockRejected(reply.orderId(), reply.reason());
            default -> orderSagaOrchestrator.onStockReleased(reply.orderId());
        }
    }

    private StockReply read(String eventType, String topic, byte[] payload) {
        if (StockReply.RESERVED.equals(eventType)) {
            GenericRecord record = avroDecoder.decode(topic, payload);
            return new StockReply(UUID.fromString(record.get("orderId").toString()), null);
        }
        return jsonMapper.readValue(payload, StockReply.class);
    }
}
