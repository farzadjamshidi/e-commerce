package com.ecommerce.order;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

// The fields the orchestrator needs from the replies of inventory
@JsonIgnoreProperties(ignoreUnknown = true)
record StockReply(UUID orderId, String reason) {

    static final String RESERVED = "StockReserved";
    static final String REJECTED = "StockRejected";
    static final String RELEASED = "StockReleased";
}
