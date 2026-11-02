package com.ecommerce.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

// Only the fields this service needs. Unknown fields are ignored, so the producer can add new ones.
@JsonIgnoreProperties(ignoreUnknown = true)
record OrderCreatedEvent(UUID orderId, String sku, int quantity) {

    static final String TYPE = "OrderCreated";
}
