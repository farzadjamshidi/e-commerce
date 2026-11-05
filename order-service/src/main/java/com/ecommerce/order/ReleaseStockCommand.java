package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

record ReleaseStockCommand(UUID commandId, String commandType, Instant issuedAt, UUID orderId) {

    static final String TYPE = "ReleaseStock";

    static ReleaseStockCommand of(UUID orderId) {
        return new ReleaseStockCommand(UUID.randomUUID(), TYPE, Instant.now(), orderId);
    }
}
