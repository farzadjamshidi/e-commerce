package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

// Only what inventory needs. The amount stays here: the orchestrator gives it to payment itself.
record ReserveStockCommand(UUID commandId, String commandType, Instant issuedAt, UUID orderId, String sku,
                           int quantity) {

    static final String TYPE = "ReserveStock";

    static ReserveStockCommand from(Order order) {
        return new ReserveStockCommand(UUID.randomUUID(), TYPE, Instant.now(), order.getId(), order.getSku(),
                order.getQuantity());
    }
}
